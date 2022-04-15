from http import HTTPStatus
import requests
import io
import pandas as pd
import json

# This class implements REDCap API methods

class REDCapConnection:
    def __init__(self, token, url):
        self.token = token #API token for the desired REDCap project
        self.url = url #REDCap instance URL
        self.primary_key = None #primary key field of the connected REDCap project
        self.record_ids = None #list of record ids in the project (values of primary key field)


    # Retrieve the primary key for the REDCap project and set the primary_key property
    def set_primary_key(self):
        data = {
            'token': self.token,
            'content': 'exportFieldNames',
            'format': 'csv',
            'returnFormat': 'json'
        }

        result = requests.post(self.url, data=data)
        if result.status_code != HTTPStatus.OK:
            print('Error: Failed to retrive the REDCap project primary key')
            print('HTTP Status: ', str(result.status_code), result.reason)
            return False
        
        # Read the result into a pandas data frame and get the primary key
        self.fields = pd.read_csv(io.StringIO(result.text))
        self.primary_key = self.fields.at[0, 'export_field_name']
        print('Primary key of the REDCap project: ', self.primary_key)

        return True


    # Retrive the list of record ids
    def export_record_ids(self):
        data = {
            'token': self.token,
            'content': 'record',
            'action': 'export',
            'format': 'csv',
            'type': 'flat',
            'fields[0]': self.primary_key,
            'returnFormat': 'json'
        }

        result = requests.post(self.url, data=data)
        if result.status_code != HTTPStatus.OK:
            print('Error: Failed to retrive the record IDs')
            print('HTTP Status: ', str(result.status_code), result.reason)
            return False
        
        self.record_ids = result.text.splitlines()
        self.record_ids.pop(0) #remove the header

        #print(self.record_ids)

        return True


    # Export records in CSV format
    def export_records_csv(self, record_ids=None):
        data = {
            'token': self.token,
            'content': 'record',
            'action': 'export',
            'format': 'csv',
            'type': 'flat',
            'returnFormat': 'json'
        }

        # If set of record ids specified, export only those records.
        if record_ids != None:
            for i in range(len(record_ids)):
                    data[f"records[{ i }]"] = record_ids[i]

        result = requests.post(self.url, data=data)
        if result.status_code != HTTPStatus.OK:
            print('Error: Failed to export records')
            print('HTTP Status: ', str(result.status_code), result.reason)
            return False
        
        #print(result.status_code, result.text)
        if not result.text.strip():
            print('Warning: No records returned for the export request')
            print('Requested record IDs: ', record_ids)
            return False

        return result.text


    # Import records in CSV format
    def import_records_csv(self, values):
        data = {
            'token': self.token,
            'content': 'record',
            'action': 'import',
            'format': 'csv',
            'type': 'flat',
            'overwriteBehavior': 'normal',
            'forceAutoNumber': 'false',
            'data': values,
            'returnContent': 'count',
            'returnFormat': 'json'
        }

        result = requests.post(self.url, data=data)
        if result.status_code != HTTPStatus.OK:
            print('Error: Failed to import records')
            print('HTTP Status: ', str(result.status_code), result.reason)
            return False
        
        num_records = json.loads(result.text)['count']
        print('Number of records imported: ', num_records)

        return num_records


    # Delete records
    def delete_records(self, record_ids):
        data = {
            'token': self.token,
            'content': 'record',
            'action': 'delete',
            'returnFormat': 'json'
        }

        # Delete the specified records.
        if record_ids != None:
            for i in range(len(record_ids)):
                    data[f"records[{ i }]"] = record_ids[i]

        result = requests.post(self.url, data=data)
        if result.status_code != HTTPStatus.OK:
            print('Error: Failed to delete records')
            print('HTTP Status: ', str(result.status_code), result.reason)
            return False
        
        print('Number of records deleted: ', result.text)

        return result.text
