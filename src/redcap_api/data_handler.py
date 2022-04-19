
import math

from configs import Configs
from redcap_api.redcap_connection import REDCapConnection

# Class to read the data from source REDCap project and write to destination REDCap project

class DataHandler:
    def __init__(self, src_prj: REDCapConnection, dest_prj: REDCapConnection):
        self.src_project = src_prj
        self.dest_project = dest_prj

    
    # Compare the source and destination project settings
    def compare_project_settings(self):
        # Compare source and destination project data-dictionaries, cannot be empty
        src_dict = self.src_project.export_data_dictionary()
        dest_dict = self.dest_project.export_data_dictionary()

        if (not src_dict) or (not dest_dict) or (src_dict != dest_dict):
            print('Error: source and destination data dictionaries are empty or do not match')
            return False

        # Compare source and destination project arms definitions
        src_arms = self.src_project.export_arms()
        dest_arms = self.dest_project.export_arms()

        if src_arms != dest_arms:
            print('Error: source and destination project arms definitions do not match')
            return False

        # Compare source and destination project event definitions
        src_events = self.src_project.export_events()
        dest_events = self.dest_project.export_events()

        if src_events != dest_events:
            print('Error: source and destination project event definitions do not match')
            return False

        # Compare source and destination project form-event mappings
        src_evnt_map = self.src_project.export_form_event_mappings()
        dest_evnt_map = self.dest_project.export_form_event_mappings()

        if src_evnt_map != dest_evnt_map:
            print('Error: source and destination project form-event mappings do not match')
            return False

        # Compare source and destination project repeating instrument definitons
        src_rpt_ins = self.src_project.export_repeating_instruments()
        dest_rpt_ins = self.dest_project.export_repeating_instruments()

        if src_rpt_ins != dest_rpt_ins:
            print('Error: source and destination project repeating instrument definitions do not match')
            return False

        return True


    # Move records from source project to destination project
    def move_data(self):
        if not self.src_project.set_primary_key():
            return

        if not self.src_project.export_record_ids():
            return

        num_records = len(self.src_project.record_ids)
        
        if num_records <= 0:
            print('Warning: No records available in the source project')
            exit(0)

        iterations = 1
        batch_size = num_records

        # Process data in batches if a valid batch size is specified
        if Configs.configs['batch_size'] > 0:
            batch_size = Configs.configs['batch_size']
            iterations = math.ceil(num_records/Configs.configs['batch_size'])
            

        i = 0
        while i < iterations:
            print('Processing batch ', i+1)

            begin = i * batch_size
            end = (i+1) * batch_size
            if end > num_records:
                end = num_records

            # Export a batch of records from the source project
            if iterations == 1:
                records = self.src_project.export_records_csv() #export all, no need to specify records IDs
            else:
                records = self.src_project.export_records_csv(self.src_project.record_ids[begin:end])

            #print(records)
            
            if records:
                # Validate the records
                if not self.validate_data(records):
                    continue

                # Import the valid records to destination project
                num_imported = self.dest_project.import_records_csv(records)
                if not num_imported:
                    break

                # Delete the records from source project
                if Configs.configs['move_records'] == 1:
                    num_deleted = self.src_project.delete_records(self.src_project.record_ids[begin:end])
                    if not num_deleted:
                        break
            else:
                break
            
            i += 1


    # Entry point to the data validation
    def validate_data(self, records):
        #TODO - implement data validation rules
        return True
