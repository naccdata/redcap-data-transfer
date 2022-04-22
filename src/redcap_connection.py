from http import HTTPStatus
from re import L
import requests
import io
import pandas as pd
import json
import logging

from configs import Configs

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
            logging.error('Failed to export the data dictionary')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
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
            logging.error('Failed to export arms definitions')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
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
            logging.error('Failed to export events definitions')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
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
            logging.error('Failed to export the form - event mappings')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
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
            logging.error('Failed to export the repeating instruments definitions')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
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
            logging.error('Failed to retrive the REDCap project primary key')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
            return False
        
        # Read the response into a pandas data frame and get the primary key
        self.fields = pd.read_csv(io.StringIO(response.text))
        self.primary_key = self.fields.at[0, 'export_field_name']
        logging.info(f'Primary key of the REDCap project: {self.primary_key}')

        return True


    # Find the list of unique record ids
    def export_record_ids(self, events=None):
        data = {
            'token': self.token,
            'content': 'record',
            'action': 'export',
            'format': 'csv',
            'type': 'flat',
            'fields[0]': self.primary_key,
            'returnFormat': 'json'
        }

        # If set of events specified, export record IDs only for those events.
        if events:
            for i in range(len(events)):
                data[f"events[{ i }]"] = events[i].strip()

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to retrive the record IDs')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
            return False

        # Read the response into a pandas data frame
        dfObj = pd.read_csv(io.StringIO(response.text))

        # Get the list of unique values in the primary key column
        # Primary key can be duplicated if the project has multiple arms/events
        self.record_ids = dfObj[self.primary_key].unique().tolist()

        #print(self.record_ids)

        return True


    # Export records in CSV format
    def export_records_csv(self, record_ids=None, forms=None, events=None):
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

        add_pk_field = False

        # If set of forms specified, export records only from those forms.
        if forms:
            for i in range(len(forms)):
                data[f"forms[{ i }]"] = forms[i].strip()
            add_pk_field = True

        # If set of events specified, export records only for those events.
        if events:
            for i in range(len(events)):
                data[f"events[{ i }]"] = events[i].strip()
            add_pk_field = True

        # If exporting only subset of forms or events, make sure to request the primary key field, need it for importing data to the destination project
        if add_pk_field:
            data['fields[0]'] = self.primary_key

        #print(data)

        response = requests.post(self.url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to export records')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
            return False
        
        #print(response.status_code, response.text)
        if not response.text.strip():
            logging.warning('No records returned for the export request')
            logging.info(f'Requested record IDs: {record_ids}')
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
            logging.error('Failed to import arms')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
            return False

        logging.info(f'Number of arms imported: {response.text}')
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
            logging.error('Failed to import events')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
            return False

        logging.info(f'Number of events imported: {response.text}')
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
            logging.error('Failed to import data dictionary')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
            return False

        logging.info(f'Number of fields imported: {response.text}')
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
            logging.error('Failed to import form event mappings')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
            return False

        logging.info(f'Number of form-event mappings imported: {response.text}')
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
            logging.error('Failed to import form event mappings')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
            return False

        logging.info(f'Number of repeating instruments/events imported: {response.text}')
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
            logging.error('Failed to import records')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
            return False
        
        num_records = json.loads(response.text)['count']
        logging.info(f'Number of records imported: {num_records}')

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
            logging.error('Failed to delete records')
            logging.info(f'HTTP Status: {str(response.status_code)} {response.reason} : {response.text}')
            return False
        
        logging.info(f'Number of records deleted: {response.text}')

        return response.text

    