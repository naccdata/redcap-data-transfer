""" DataHandler module """

import json
import logging
import math
import pandas as pd

from redcap_connection import REDCapConnection
from validator.quality_check import QualityCheck


class DataHandler:
    """ Class to read the data from a source REDCap project
        and write to a destination REDCap project
    """

    def __init__(self, src_prj: REDCapConnection, dest_prj: REDCapConnection):
        self.src_project: REDCapConnection = src_prj
        self.dest_project: REDCapConnection = dest_prj
        self.data_dict: pd.DataFrame = None
        self.qual_check: QualityCheck = None

    def compare_project_settings(self, forms: list[str] = None) -> bool:
        """ Compare the source and destination project settings """

        # Compare source and destination project data-dictionaries, cannot be empty
        src_dict = self.src_project.export_data_dictionary(forms)
        dest_dict = self.dest_project.export_data_dictionary(forms)

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

    def set_quality_checker(self, error_log: str) -> bool:
        """ Set up QualityCheck instance to run data validation rules """

        try:
            self.qual_check = QualityCheck(self.src_project.primary_key,
                                           error_log)
            return True
        except FileNotFoundError as e:
            logging.critical('Failed to set up error log file - %s : %s',
                             error_log, e.strerror)
            return False

    def transfer_data(self,
                      batch_size_val: int,
                      move_records: int = 0,
                      forms: list[str] = None,
                      events: list[str] = None):
        """ Move/copy records from source project to destination project """

        if not self.src_project.export_record_ids(forms, events):
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
        while i < iterations:
            logging.info('Processing batch %s ...........', i + 1)

            begin = i * batch_size
            end = (i + 1) * batch_size
            end = min(end, num_records)

            # Export a batch of records from the source project
            if (iterations == 1) and (not forms) and (not events):
                # If there is only one iteration and no filtering, export all
                records = self.src_project.export_records(exp_format='json')
            else:
                records = self.src_project.export_records(
                    'json', self.src_project.record_ids[begin:end], forms,
                    events)

            if records:
                # Validate the records
                valid_records = self.validate_data(records)
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
                    break

                # Delete the records from source project
                if move_records == 1:
                    num_deleted = self.src_project.delete_records(
                        self.src_project.record_ids[begin:end])
                    if not num_deleted:
                        break

            i += 1

    def get_form_name(self, var_name: str) -> str:
        """ Find the form name for a given field """

        row = self.data_dic[self.data_dic['field_name'] == var_name]
        return row['form_name'].values[0]

    def validate_data(self, records: str) -> list[str]:
        """ Entry point to the data validation """

        valid_records = []
        input_records = json.loads(records)

        # Check each record against the defined rules
        for record in input_records:
            if self.qual_check.check_record(record):
                valid_records.append(record)

        return valid_records
