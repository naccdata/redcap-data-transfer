from http import HTTPStatus
from urllib import response
import requests
import io
import pandas as pd
import json

# This class implements the REDCap API methods

class REDCapConnection:
    def __init__(self, token, url):
        self.token = token #API token for the desired REDCap project
        self.url = url #REDCap instance URL
        self.primary_key = None #primary key field of the connected REDCap project
        self.record_ids = None #list of record ids in the project (values of primary key field)


    # Export the project data-dictionary in JSON format
    def export_data_dictionary(self):
        data = {
            'token': self.token,
            'content': 'metadata',
            'format': 'json',
            'returnFormat': 'json'
        }

        response = requests.post(self.url, data=data)
            
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to export the data dictionary')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False

        return response.text


    # Export the arm definitions of the project in JSON format
    def export_arms(self):
        data = {
            'token': self.token,
            'content': 'arm',
            'format': 'json'
        }

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to export arms definitions')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False

        return response.text


    # Export the event definitions of the project in JSON format
    def export_events(self):
        data = {
            'token': self.token,
            'content': 'event',
            'format': 'json'
        }

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to export events definitions')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False

        return response.text
    

    # Export the form - event mappings in the project in JSON format
    def export_form_event_mappings(self):
        data = {
            'token': self.token,
            'content': 'formEventMapping',
            'format': 'json',
            'returnFormat': 'json'
        }

        response = requests.post(self.url, data=data)
            
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to export the form - event mappings')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False

        return response.text


    # Export the repeating instruments and event definitions in the project in JSON format
    def export_repeating_instruments(self):
        data = {
            'token': self.token,
            'content': 'repeatingFormsEvents',
            'format': 'json',
            'returnFormat': 'json'
        }

        response = requests.post(self.url, data=data)
            
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to export the repeating instruments definitions')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False

        return response.text


    # Find the primary key for the REDCap project and set the primary_key property
    def set_primary_key(self):
        data = {
            'token': self.token,
            'content': 'exportFieldNames',
            'format': 'csv',
            'returnFormat': 'json'
        }

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to retrive the REDCap project primary key')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False
        
        # Read the response into a pandas data frame and get the primary key
        self.fields = pd.read_csv(io.StringIO(response.text))
        self.primary_key = self.fields.at[0, 'export_field_name']
        print('Primary key of the REDCap project: ', self.primary_key)

        return True


    # Find the list of unique record ids
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

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to retrive the record IDs')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False
        
        #self.record_ids = response.text.splitlines()
        #self.record_ids.pop(0) #remove the header

        # Read the response into a pandas data frame
        dfObj = pd.read_csv(io.StringIO(response.text))

        # Get the list of unique values in the primary key column
        # Primary key can be duplicated if the project has multiple arms/events
        self.record_ids = dfObj[self.primary_key].unique().tolist()

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
        if record_ids:
            for i in range(len(record_ids)):
                data[f"records[{ i }]"] = record_ids[i]

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to export records')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False
        
        #print(response.status_code, response.text)
        if not response.text.strip():
            print('Warning: No records returned for the export request')
            print('Requested record IDs: ', record_ids)
            return False

        return response.text


    # Import project arms definitions in JSON format
    # Delete all existing arms and import
    def import_arms(self, arms):
        data = {
            'token': self.token,
            'content': 'arm',
            'action': 'import',
            'format': 'json',
            'override': 1, 
            'data': arms
        }

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to import arms')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False

        print('Number of arms imported: ', response.text)
        return response.text


    # Import project event definitions in JSON format
    # Delete all existing events and import
    def import_events(self, events):
        data = {
            'token': self.token,
            'content': 'event',
            'action': 'import',
            'format': 'json',
            'override': 1,
            'data': events
        }

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to import events')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False

        print('Number of events imported: ', response.text)
        return response.text


    # Import project data-dictionary in JSON format
    def import_data_dictionary(self, data_dict):
        data = {
            'token': self.token,
            'content': 'metadata',
            'format': 'json',
            'data': data_dict,
            'returnFormat': 'json'
        }

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to import data dictionary')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False

        print('Number of fields imported: ', response.text)
        return response.text
    
    
    # Import project instrument-event mappings in JSON format
    def import_form_event_mappings(self, form_event_map):
        data = {
            'token': self.token,
            'content': 'formEventMapping',
            'data': form_event_map,
            'format': 'json',
            'returnFormat': 'json'
        }

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to import form event mappings')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False

        print('Number of form-event mappings imported: ', response.text)
        return response.text


    # Import project repeating instrument definitions in JSON format
    def import_repeating_instruments(self, repeating_ins):
        data = {
            'token': self.token,
            'content': 'repeatingFormsEvents',
            'data': repeating_ins,
            'format': 'json',
            'returnFormat': 'json'
        }

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to import form event mappings')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False

        print('Number of repeating instruments/events imported: ', response.text)
        return response.text


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

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to import records')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False
        
        num_records = json.loads(response.text)['count']
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

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            print('Error: Failed to delete records')
            print('HTTP Status: ', str(response.status_code), response.reason, ' : ', response.text)
            return False
        
        print('Number of records deleted: ', response.text)

        return response.text

    