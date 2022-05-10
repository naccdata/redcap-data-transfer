""" DataHandler module """

import logging
import math

from redcap_connection import REDCapConnection


class DataHandler:
    """ Class to read the data from a source REDCap project
        and write to a destination REDCap project
    """

    def __init__(self, src_prj: REDCapConnection, dest_prj: REDCapConnection):
        self.src_project: REDCapConnection = src_prj
        self.dest_project: REDCapConnection = dest_prj

    def compare_project_settings(self, forms: list[str] = None) -> bool:
        """ Compare the source and destination project settings """

        src_attr = self.src_project.project_attr
        dest_attr = self.dest_project.project_attr

        # Compare source and destination project data-dictionaries, cannot be empty
        src_dict = self.src_project.export_data_dictionary(forms)
        dest_dict = self.dest_project.export_data_dictionary(forms)

        if (not src_dict) or (not dest_dict) or (src_dict != dest_dict):
            logging.error(
                'Source and destination data dictionaries are empty or do not match'
            )
            return False

        # Compare source and destination project longitudinal settings
        if src_attr['is_longitudinal'] and dest_attr['is_longitudinal']:
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
        elif src_attr['is_longitudinal'] != dest_attr['is_longitudinal']:
            logging.error(
                'Source and destination project longitudinal settings do not match'
            )
            return False

        # Compare source and destination project repeating instrument settings
        if src_attr['has_repeating_instruments_or_events'] and dest_attr[
                'has_repeating_instruments_or_events']:
            src_rpt_ins = self.src_project.export_repeating_instruments()
            dest_rpt_ins = self.dest_project.export_repeating_instruments()

            if src_rpt_ins != dest_rpt_ins:
                logging.error(
                    'Source and destination project repeating instrument definitions do not match'
                )
                return False
        elif src_attr['has_repeating_instruments_or_events'] != dest_attr[
                'has_repeating_instruments_or_events']:
            logging.error(
                'Source and destination project repeated instruments settings do not match'
            )
            return False

        return True

    def move_data(self,
                  batch_size_val: int,
                  move_records: int = 0,
                  forms: list[str] = None,
                  events: list[str] = None):
        """ Move records from source project to destination project """

        if not self.src_project.export_record_ids(events):
            return

        num_records = len(self.src_project.record_ids)

        if num_records <= 0:
            logging.warning(
                'No records available in the source project matching to the specifications'
            )
            return

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
                records = self.src_project.export_records(exp_format='csv')
            else:
                records = self.src_project.export_records(
                    'csv', self.src_project.record_ids[begin:end], forms,
                    events)

            if records:
                # Validate the records
                if not self.validate_data(records):
                    continue

                # Import the valid records to destination project
                num_imported = self.dest_project.import_records_csv(records)
                if not num_imported:
                    break

                # Delete the records from source project
                if move_records == 1:
                    num_deleted = self.src_project.delete_records(
                        self.src_project.record_ids[begin:end])
                    if not num_deleted:
                        break

            i += 1

    def validate_data(self, records) -> bool:
        """ Entry point to the data validation """       
        return True
