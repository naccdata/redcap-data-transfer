
import math

from configs import Configs
from redcap_api.redcap_connection import REDCapConnection

# Class to read the data from source REDCap project and write to destination REDCap project

class DataHandler:
    def __init__(self, src_prj: REDCapConnection, dest_prj: REDCapConnection):
        self.src_project = src_prj
        self.dest_project = dest_prj

    
    def move_data(self):
        if self.src_project.set_primary_key() == False:
            return

        if self.src_project.export_record_ids() == False:
            return

        num_records = len(self.src_project.record_ids)
        
        if num_records <=0:
            print('No records available in the source project')
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

            if records != False:
                # Validate the records
                if self.validate_data(records) == False:
                    continue

                # Import the valid records to destination project
                num_imported = self.dest_project.import_records_csv(records)
                if num_imported == False:
                    break

                # Delete the records from source project
                if Configs.configs['move_records'] == 1:
                    num_deleted = self.src_project.delete_records(self.src_project.record_ids[begin:end])
                    if num_deleted == False:
                        break
            else:
                break
            i += 1

    def validate_data(self, records):
        #TODO - implement data validation rules
        return True
