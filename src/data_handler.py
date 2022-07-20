""" DataHandler module """

import html2text
import json
import logging
import math
import pandas as pd

from datetime import datetime as dt
from typing import Tuple

from redcap_connection import REDCapConnection, REDCapKeys
from validator.quality_check import QualityCheck, QualityCheckException


class DataHandler:
    """ Class to read the data from a source REDCap project
        and write to a destination REDCap project
    """

    def __init__(self,
                 src_prj: REDCapConnection,
                 dest_prj: REDCapConnection,
                 forms: list[str] = None,
                 events: list[str] = None,
                 qc_err_form: str = None):
        """

        Args:
            src_prj (REDCapConnection): Source REDCap connection
            dest_prj (REDCapConnection): Destination REDCap connection
            forms (list[str], optional): List of form names to be included in data transfer.
            events (list[str], optional): List of events to be included in data transfer.
            qc_err_form (str, optional): Name of the REDCap form for reporting validation errors.
        """

        self._src_project: REDCapConnection = src_prj
        self._dest_project: REDCapConnection = dest_prj
        self._qc_err_form = qc_err_form

        # List of all form names in the source project with labels
        self._all_forms: list[dict[str, str]] = src_prj.export_froms_list()
        # If forms not specified, all forms will be included
        self._forms: list[str] = forms if forms else self.get_forms_list()
        # If events not specified all events will be included
        self._events: list[str] = events
        # Data dictionary for the data entry forms
        # this includes all metadata related to form fields
        self._data_dict: pd.DataFrame = None
        # Object to validate data quality rules
        self._qual_check: QualityCheck = None

    def get_forms_list(self) -> list[str]:
        """ Get the list of forms in the source project.

        Returns:
            list[str]: List of form names
        """

        forms_list = []
        if self._all_forms:
            for form in self._all_forms:
                forms_list.append(form[REDCapKeys.INST_NAME])

        # Exclude the validation error reporting form
        if self._qc_err_form:
            forms_list.remove(self._qc_err_form)

        return forms_list

    def get_form_name(self, var_name: str) -> str:
        """ Find the name of the form that the given variable belongs to.

        Args:
            var_name (str): Variable name

        Returns:
            str: Form name
        """

        row = self._data_dict[self._data_dict[REDCapKeys.FLD_NAME] == var_name]
        return row[REDCapKeys.FORM_NAME].values[0]

    def get_field_label(self, var_name: str) -> str:
        """ Find the label for a given field.

        Args:
            var_name (str): Variable name

        Returns:
            str: Field label
        """

        row = self._data_dict[self._data_dict[REDCapKeys.FLD_NAME] == var_name]
        return row[REDCapKeys.FLD_LBL].values[0]

    def compare_project_settings(self) -> bool:
        """ Compare the source and destination project settings.

        Returns:
            bool: True if project settings match, else False
        """

        # Compare source and destination project data-dictionaries, cannot be empty
        src_dict = self._src_project.export_data_dictionary(self._forms)
        dest_dict = self._dest_project.export_data_dictionary(self._forms)

        if (not src_dict) or (not dest_dict) or (src_dict != dest_dict):
            logging.error(
                'Source and destination data dictionaries are empty or do not match'
            )
            return False

        # Set data dictionary
        self._data_dict = pd.read_json(src_dict)

        # Compare source and destination project longitudinal settings
        src_lng = self._src_project.is_longitudinal()
        dest_lng = self._dest_project.is_longitudinal()
        if (src_lng and not dest_lng) or (dest_lng and not src_lng):
            logging.error(
                'Source and destination project longitudinal settings do not match'
            )
            return False

        if src_lng and dest_lng:
            # Compare source and destination project arms definitions
            src_arms = self._src_project.export_arms()
            dest_arms = self._dest_project.export_arms()

            if src_arms != dest_arms:
                logging.error(
                    'Source and destination project arms definitions do not match'
                )
                return False

            # Compare source and destination project event definitions
            src_events = self._src_project.export_events()
            dest_events = self._dest_project.export_events()

            if src_events != dest_events:
                logging.error(
                    'Source and destination project event definitions do not match'
                )
                return False

            # Compare source and destination project form-event mappings
            src_evnt_map = self._src_project.export_form_event_mappings()
            dest_evnt_map = self._dest_project.export_form_event_mappings()

            if src_evnt_map != dest_evnt_map:
                logging.error(
                    'Source and destination project form-event mappings do not match'
                )
                return False

        # Compare source and destination project repeating instrument settings
        src_ins = self._src_project.has_repeating_instruments()
        dest_ins = self._dest_project.has_repeating_instruments()
        if (src_ins and not dest_ins) or (dest_ins and not src_ins):
            logging.error(
                'Source and destination project repeated instruments settings do not match'
            )
            return False

        if src_ins and dest_ins:
            src_rpt_ins = self._src_project.export_repeating_instruments()
            dest_rpt_ins = self._dest_project.export_repeating_instruments()

            if src_rpt_ins != dest_rpt_ins:
                logging.error(
                    'Source and destination project repeating instrument definitions do not match'
                )
                return False

        return True

    def set_quality_checker(self, rules_dir: str, strict: bool = True) -> bool:
        """ Set up QualityCheck instance to run data validation rules.

        Args:
            rules_dir (str): Location where rule definitions are stored
            strict (bool, optional): Validation mode. Defaults to True

        Returns:
            bool: False if QualityCheckException occurs, else True
        """

        try:
            self._qual_check = QualityCheck(self._src_project.primary_key,
                                            rules_dir, self._forms, strict)
            return True
        except QualityCheckException as e:
            logging.critical(e)
            return False

    def transfer_data(self, batch_size_val: int, move_records: bool = True):
        """ Move/copy records from source project to destination project.

        Args:
            batch_size_val (int): Number of records to be processed at a time
            move_records (bool, optional): Move records to destination and delete from source. Defaults to True.
        """

        if not self._src_project.export_record_ids(self._forms, self._events):
            return

        num_records = len(self._src_project.record_ids)

        if num_records <= 0:
            logging.warning(
                'No records available in the source project matching to the specifications'
            )
            return

        logging.info('Number of records available in the source project: %s',
                     num_records)

        iterations = 1
        batch_size = num_records

        # Process data in batches if a valid batch size is specified
        if batch_size_val > 0:
            batch_size = batch_size_val
            iterations = math.ceil(num_records / batch_size_val)

        i = 0
        total_imported = 0
        while i < iterations:
            begin = i * batch_size
            end = (i + 1) * batch_size
            end = min(end, num_records)

            logging.info('Processing batch %s of %s records ...........',
                         i + 1, end - begin)

            if iterations == 1:
                # If there is only one iteration, export all.
                # no need to specify record ids
                record_ids = None
            else:
                record_ids = self._src_project.record_ids[begin:end]

            # Export a batch of records from the source project
            records = self._src_project.export_records('json', record_ids,
                                                       self._forms,
                                                       self._events)
            if records:
                # Validate the records
                valid_ids, valid_records, failed_records = self.validate_data(
                    records)

                if len(valid_records) == 0:
                    logging.info('There are no valid records in batch %s ',
                                 i + 1)
                    i += 1
                    continue

                # Import the valid records to destination project
                import_json_str = json.dumps(valid_records)
                num_imported = self._dest_project.import_records(
                    import_json_str, 'json')
                if num_imported:
                    logging.info(
                        'Number of records imported to the destination project: %s',
                        num_imported)
                    total_imported += num_imported
                else:
                    i += 1
                    continue

                # Delete the valid records from source project if move records enabled
                if move_records:
                    # For longitudinal data, this will delete the entire record (including the failed instances, if any)
                    # Any failed instances will be added back to the source project in the next step
                    num_deleted = self.move_records(valid_ids)
                    if num_deleted:
                        logging.info(
                            'Number of records deleted from the source project: %s',
                            num_deleted)

                # Import back the failed records to the source project with validation errors
                if len(failed_records) != 0:
                    errors_json_str = json.dumps(failed_records)
                    if not self._src_project.import_records(
                            errors_json_str, 'json'):
                        logging.warning(
                            'Failed to write validation errors to the source project'
                        )

            i += 1

        logging.info(
            'Total number of records successfully imported to the destination project: %s',
            total_imported)
        logging.info(
            'Number of records failed due to validation or import erros: %s',
            num_records - total_imported)

    def validate_data(
        self, records_str: str
    ) -> Tuple[list[str], list[dict[str, str]], list[dict[str, str]]]:
        """ Entry point to the data validation.

        Args:
            records_str (str): List of input records as a JSON format string

        Returns:
            list[str]: List of valid record IDs,
            list[dict[str, str]]: List of valid records, 
            list[dict[str, str]]: List of failed records with error messages
        """

        valid_records = []
        failed_records = []
        valid_ids = set()
        records_list = json.loads(records_str)

        logging.info('Num input instances: %s', len(records_list))
        # Check each record against the defined rules
        for record in records_list:
            record_id = record[self._src_project.primary_key]
            valid, dict_erros = self._qual_check.check_record_cerberus(record)
            if valid:
                valid_records.append(record)
                valid_ids.add(record_id)
            else:
                self.compose_error_report_for_record(record, dict_erros,
                                                     failed_records)

        logging.info('Num valid instances: %s', len(valid_records))
        logging.info('Num failed instances: %s', len(failed_records))

        return list(valid_ids), valid_records, failed_records

    def compose_error_report_for_record(self, record: dict[str, str],
                                        errors: dict[str, list[str]],
                                        failed_records: list[dict[str, str]]):
        """ Create an object to report the validation errors, 
            these will be imported to REDCap project in JSON format

        Args:
            record (dict[str, str]): Failed record
            errors (dict[str, list[str]]): Error list for each variable
            failed_records (list[dict[str, str]]): Failed records list, to append the generated error report
        """
        src_rdcp = self._src_project
        record_id = record[src_rdcp.primary_key]
        err_header = f'Validation failed for the record {src_rdcp.primary_key} = {record_id}'

        failed_rec = record.copy()
        if src_rdcp.is_longitudinal or src_rdcp.has_repeating_instruments:
            if REDCapKeys.RDCP_EVENT_NAME in record and record[
                    REDCapKeys.RDCP_EVENT_NAME]:
                err_header += f', event = {record[REDCapKeys.RDCP_EVENT_NAME]}'
            if REDCapKeys.RDCP_RPT_INSTR in record and record[
                    REDCapKeys.RDCP_RPT_INSTR]:
                err_header += f', instrument = {record[REDCapKeys.RDCP_RPT_INSTR]}'
            if REDCapKeys.RDCP_RPT_INSTN in record and record[
                    REDCapKeys.RDCP_RPT_INSTN]:
                err_header += f', instance = {record[REDCapKeys.RDCP_RPT_INSTN]}'

        error_str = ''
        for key in errors:
            error_str += 'Form Name: ' + self.get_form_name(key) + ' | '
            error_str += 'Question: ' + html2text.html2text(
                self.get_field_label(key)).strip() + ' | '
            error_str += 'Variable: ' + key + ' | '
            error_str += 'Current value: ' + str(record[key]) + ' | '
            error_str += 'Errors: ' + str(errors[key]) + '\n'

        if self._qc_err_form:
            failed_rec['timestamp'] = (dt.now()).strftime('%m-%d-%y %H:%M:%S')
            failed_rec['val_errs'] = error_str

        failed_records.append(failed_rec)

        logging.error('%s. List of errors:', err_header)
        logging.info(error_str)

    def move_records(self, record_ids: list[int | str]) -> (bool | int):
        """ Delete records from the project.

        Args:
            record_ids (list[int  |  str]): List of record IDs to be deleted

        Returns:
            int | bool: Number of records deleted or False if an error occured
        """

        is_subset = len(self._forms) < (len(self._all_forms) - 1)

        if is_subset:
            logging.warning(
                'Records not removed from the source project as only a subset of the forms are validated'
            )
            return False
        else:
            return self._src_project.delete_records(record_ids)
