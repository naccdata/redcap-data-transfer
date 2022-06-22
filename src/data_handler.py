""" DataHandler module """

import json
import logging
import math
import pandas as pd

from datetime import datetime as dt
from typing import Tuple

from redcap_connection import REDCapConnection
from validator.quality_check import QualityCheck


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
        self.src_project: REDCapConnection = src_prj
        self.dest_project: REDCapConnection = dest_prj
        # List of all form names in the source project with labels
        self.all_forms: list[dict[str, str]] = src_prj.export_froms_list()
        # Name of the REDCap form for reporting validation errors
        self.qc_err_form = qc_err_form
        # List of form names to be included in data transfer,
        # if not specified, all forms will be included
        self.forms: list[str] = forms if forms else self.get_forms_list()
        # List of events to be included in data transfer,
        # if not specified all events will be included
        self.events: list[str] = events
        # Data dictionary for the data entry forms
        # this includes all metadata related to form fields
        self.data_dict: pd.DataFrame = None
        # Object to validate data quality rules
        self.qual_check: QualityCheck = None

    def get_forms_list(self) -> list[str]:
        """ Get the list of forms in source project """
        forms_list = []

        if self.all_forms:
            for form in self.all_forms:
                forms_list.append(form['instrument_name'])

        # Exclude the validation error reporting form
        if self.qc_err_form:
            forms_list.remove(self.qc_err_form)

        return forms_list

    def get_form_name(self, var_name: str) -> str:
        """ Find the form name for a given field """

        row = self.data_dic[self.data_dic['field_name'] == var_name]
        return row['form_name'].values[0]

    def get_field_label(self, var_name: str) -> str:
        """ Find the label for a given field """

        row = self.data_dic[self.data_dic['field_name'] == var_name]
        return row['field_label'].values[0]

    def compare_project_settings(self) -> bool:
        """ Compare the source and destination project settings """

        # Compare source and destination project data-dictionaries, cannot be empty
        src_dict = self.src_project.export_data_dictionary(self.forms)
        dest_dict = self.dest_project.export_data_dictionary(self.forms)

        if (not src_dict) or (not dest_dict) or (src_dict != dest_dict):
            logging.error(
                'Source and destination data dictionaries are empty or do not match'
            )
            return False

        # Set data dictionary
        self.data_dic = pd.read_json(src_dict)

        # Compare source and destination project longitudinal settings
        src_lng = self.src_project.is_longitudinal()
        dest_lng = self.dest_project.is_longitudinal()
        if (src_lng and not dest_lng) or (dest_lng and not src_lng):
            logging.error(
                'Source and destination project longitudinal settings do not match'
            )
            return False

        if src_lng and dest_lng:
            # Compare source and destination project arms definitions
            src_arms = self.src_project.export_arms()
            dest_arms = self.dest_project.export_arms()

            if src_arms != dest_arms:
                logging.error(
                    'Source and destination project arms definitions do not match'
                )
                return False

            # Compare source and destination project event definitions
            src_events = self.src_project.export_events()
            dest_events = self.dest_project.export_events()

            if src_events != dest_events:
                logging.error(
                    'Source and destination project event definitions do not match'
                )
                return False

            # Compare source and destination project form-event mappings
            src_evnt_map = self.src_project.export_form_event_mappings()
            dest_evnt_map = self.dest_project.export_form_event_mappings()

            if src_evnt_map != dest_evnt_map:
                logging.error(
                    'Source and destination project form-event mappings do not match'
                )
                return False

        # Compare source and destination project repeating instrument settings
        src_ins = self.src_project.has_repeating_instruments()
        dest_ins = self.dest_project.has_repeating_instruments()
        if (src_ins and not dest_ins) or (dest_ins and not src_ins):
            logging.error(
                'Source and destination project repeated instruments settings do not match'
            )
            return False

        if src_ins and dest_ins:
            src_rpt_ins = self.src_project.export_repeating_instruments()
            dest_rpt_ins = self.dest_project.export_repeating_instruments()

            if src_rpt_ins != dest_rpt_ins:
                logging.error(
                    'Source and destination project repeating instrument definitions do not match'
                )
                return False

        return True

    def set_quality_checker(self, rules_dir: str):
        """ Set up QualityCheck instance to run data validation rules """

        self.qual_check = QualityCheck(self.src_project.primary_key, rules_dir,
                                       self.forms)

    def transfer_data(self, batch_size_val: int, move_records: int = 0):
        """ Move/copy records from source project to destination project """

        if not self.src_project.export_record_ids(self.forms, self.events):
            return

        num_records = len(self.src_project.record_ids)

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

            # Export a batch of records from the source project
            if (iterations == 1) and (not self.forms) and (not self.events):
                # If there is only one iteration and no filtering, export all
                records = self.src_project.export_records(exp_format='json')
            else:
                records = self.src_project.export_records(
                    'json', self.src_project.record_ids[begin:end], self.forms,
                    self.events)

            if records:
                # Validate the records
                valid_ids, valid_records, failed_records = self.validate_data(
                    records)

                # Import the validation errors to the source project
                if self.qc_err_form and len(failed_records) != 0:
                    errors_json_str = json.dumps(failed_records)
                    self.src_project.import_records(errors_json_str, 'json')

                if len(valid_records) == 0:
                    logging.info('There are no valid records in batch %s ',
                                 i + 1)
                    i += 1
                    continue

                # Import the valid records to destination project
                import_json_str = json.dumps(valid_records)
                num_imported = self.dest_project.import_records(
                    import_json_str, 'json')
                if not num_imported:
                    i += 1
                    continue
                else:
                    total_imported += num_imported

                # Delete the valid records from source project if move records enabled
                if move_records == 1:
                    num_deleted = self.src_project.delete_records(valid_ids)
                    if not num_deleted:
                        break

            i += 1

        logging.info(
            'Total number of records successfully imported to the destination project: %s',
            total_imported)
        logging.info(
            'Number of records failed due to validation or import erros: %s',
            num_records - total_imported)

    def validate_data(
        self, records: str
    ) -> Tuple[list[str], list[dict[str, str]], list[dict[str, str]]]:
        """ Entry point to the data validation """

        valid_records = []
        failed_records = []
        valid_ids = set()
        input_records = json.loads(records)

        # Check each record against the defined rules
        for record in input_records:
            record_id = record[self.src_project.primary_key]
            valid, dict_erros = self.qual_check.check_record(record)
            if valid:
                valid_records.append(record)
                valid_ids.add(record_id)
            else:
                self.compose_error_report_for_record(record_id, dict_erros,
                                                     failed_records)

        return list(valid_ids), valid_records, failed_records

    def compose_error_report_for_record(self, record_id: str,
                                        errors: dict[str, str],
                                        failed_records: list[dict[str, str]]):
        """ Create an object to report the validation errors, 
            these will be imported to REDCap project in JSON format
        """

        failed = {}
        failed[self.src_project.primary_key] = record_id
        failed['timestamp'] = (dt.now()).strftime('%m-%d-%y %H:%M:%S')
        error_str = ''
        for key in errors:
            error_str += 'Form Name: ' + self.get_form_name(key) + ' | '
            error_str += 'Question: ' + self.get_field_label(key) + ' | '
            error_str += 'Errors: ' + errors[key] + '\n'
        failed['val_errs'] = error_str
        failed_records.append(failed)

        logging.error(
            'Validation failed for the record %s = %s, list of errors:',
            self.src_project.primary_key, record_id)
        logging.info(error_str)
