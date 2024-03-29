""" REDCap module """

import io
import json
import logging
import pandas as pd
import requests

from http import HTTPStatus
from json.decoder import JSONDecodeError


class REDCapConnectionException(Exception):
    """ Raised if something goes wrong with API calls """

    pass


class REDCapKeys:
    """ Class to store REDCap attribute labels """

    RDCP_EVENT_NAME = 'redcap_event_name'
    RDCP_RPT_INSTR = 'redcap_repeat_instrument'
    RDCP_RPT_INSTN = 'redcap_repeat_instance'
    INST_NAME = 'instrument_name'
    FORM_NAME = 'form_name'
    FLD_NAME = 'field_name'
    FLD_LBL = 'field_label'
    FLD_TYPE = 'field_type'
    FLD_CHOICES = 'select_choices_or_calculations'


class REDCapConnection:
    """ This class implements the REDCap API methods """

    def __init__(self, token: str, url: str):
        """

        Args:
            token (str): API token for the REDCap project
            url (str): REDCap instance URL

        Raises:
            REDCapConnectionException: If there is an error connecting to the specified project
        """

        self.__token: str = token
        self.__url: str = url

        # RedCAP project attributes
        self.__project_attr: dict[str, object] = None
        # Primary key field of the connected REDCap project
        self.__primary_key: str = None
        # List of record ids in the project (values of primary key field)
        self.__record_ids: list[str | int] = None

        if not self.__set_project_info():
            raise REDCapConnectionException

        if not self.__set_primary_key():
            raise REDCapConnectionException

    @property
    def primary_key(self) -> str:
        """ The primary_key property

        Returns:
            str: Primary key field of the REDCap project
        """
        return self.__primary_key

    @property
    def project_attr(self) -> dict[str, object]:
        """ The project_attr property

        Returns:
            dict[str, object]: Attributes of the REDCap project
        """
        return self.__project_attr

    @property
    def record_ids(self) -> list[str | int]:
        """ The record_ids property

        Returns:
            list[str | int]: List of record ids (primary keys) in the project
        """
        return self.__record_ids

    def __set_project_info(self) -> bool:
        """ Export project info and set the project_attr property.

        Returns:
            bool: True if project metadata successfully exported, else False
        """

        data = {
            'token': self.__token,
            'content': 'project',
            'format': 'json',
            'returnFormat': 'json'
        }

        response = requests.post(self.__url, data=data)

        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to export project information')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        try:
            self.__project_attr = response.json()
        except JSONDecodeError as e:
            logging.critical('Error in parsing project information: %s', e)
            return False

        return True

    def __set_primary_key(self) -> bool:
        """ Find the primary key of the REDCap project and set the primary_key property.

        Returns:
            bool: True if project primary key successfully set, else False
        """

        data = {
            'token': self.__token,
            'content': 'exportFieldNames',
            'format': 'csv',
            'returnFormat': 'json'
        }

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to retrive the REDCap project primary key')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        # Read the response into a pandas data frame and get the primary key
        df_fields = pd.read_csv(io.StringIO(response.text))
        self.__primary_key = df_fields.at[0, 'export_field_name']

        return True

    def get_project_id(self) -> int:
        """ Get the project title.

        Returns:
            int: Project ID
        """

        return self.__project_attr['project_id']

    def get_project_title(self) -> str:
        """ Get the project title.

        Returns:
            str: Project title
        """

        return self.__project_attr['project_title']

    def is_longitudinal(self) -> bool:
        """ Get the longitudinal setting for the project.

        Returns:
            bool: True if longitudinal setting enabled
        """

        return bool(self.__project_attr['is_longitudinal'])

    def has_repeating_instruments(self) -> bool:
        """ Get the repeating instruments setting for the project.

        Returns:
            bool: True if repeating instruments enabled
        """

        return bool(self.__project_attr['has_repeating_instruments_or_events'])

    def export_data_dictionary(self, forms: list[str] = None) -> str | bool:
        """ Export the project data-dictionary in JSON format.

        Args:
            forms (list[str], optional): List of forms to be included

        Returns:
            str | bool: Data dictionary in JSON format string or False if an error occured
        """

        data = {
            'token': self.__token,
            'content': 'metadata',
            'format': 'json',
            'returnFormat': 'json'
        }

        # If set of forms specified, export metadata from those forms.
        if forms:
            for i, form in enumerate(forms):
                data[f'forms[{ i }]'] = form

        response = requests.post(self.__url, data=data)

        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to export the data dictionary')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        return response.text

    def export_froms_list(self) -> list[dict[str, str]]:
        """ Export the list of data entry forms in the project.

        Returns:
            list[dict[str, str]]: List of forms [form_name, form_label]
        """

        data = {
            'token': self.__token,
            'content': 'instrument',
            'format': 'json'
        }

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to export data entry forms list')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return None

        try:
            return response.json()
        except JSONDecodeError as e:
            logging.error('Error in parsing the data entry forms list: %s', e)
            return None

    def export_arms(self) -> str | bool:
        """ Export the arm definitions of the project in JSON format.

        Returns:
            str | bool: Arms definitons in JSON format string or False if an error occured
        """

        data = {'token': self.__token, 'content': 'arm', 'format': 'json'}

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to export arms definitions')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        return response.text

    def export_events(self) -> str | bool:
        """ Export the event definitions of the project in JSON format.

        Returns:
            str | bool: Event definitons in JSON format string or False if an error occured
        """

        data = {'token': self.__token, 'content': 'event', 'format': 'json'}

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to export events definitions')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        return response.text

    def export_form_event_mappings(self) -> str | bool:
        """ Export the form - event mappings in the project in JSON format.

        Returns:
            str | bool: Form-Event mappings in JSON format string or False if an error occured
        """

        data = {
            'token': self.__token,
            'content': 'formEventMapping',
            'format': 'json',
            'returnFormat': 'json'
        }

        response = requests.post(self.__url, data=data)

        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to export the form - event mappings')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        return response.text

    def export_repeating_instruments(self) -> str | bool:
        """ Export the repeating instruments and event definitions in the project in JSON format.

        Returns:
            str | bool: Repeating instruments defs in JSON format string or False if an error occured
        """

        data = {
            'token': self.__token,
            'content': 'repeatingFormsEvents',
            'format': 'json',
            'returnFormat': 'json'
        }

        response = requests.post(self.__url, data=data)

        if response.status_code != HTTPStatus.OK:
            logging.error(
                'Failed to export the repeating instruments definitions')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        return response.text

    def export_record_ids(self,
                          forms: list[str] = None,
                          events: list[str] = None) -> bool:
        """ Find the list of unique record ids and set the record_ids property.

        Args:
            forms (list[str], optional): List of forms to be included
            events (list[str], optional): List of events to be included

        Returns:
            bool: True if unique record ids successfully exported, else False
        """

        data = {
            'token': self.__token,
            'content': 'record',
            'action': 'export',
            'format': 'csv',
            'type': 'flat',
            'fields[0]': self.__primary_key,
            'returnFormat': 'json'
        }

        # If set of forms specified, export records only from those forms.
        if forms:
            for i, form in enumerate(forms):
                data[f'forms[{ i }]'] = form

        # If set of events specified, export record IDs only for those events.
        if events:
            for i, event in enumerate(events):
                data[f'events[{ i }]'] = event

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to retrive the record IDs')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        if not response.text.strip():
            logging.warning(
                'Source project does not have any matching records')
            return False

        # Read the response into a pandas data frame
        df_obj = pd.read_csv(io.StringIO(response.text))

        # Get the list of unique values in the primary key column
        # Primary key can be duplicated if the project has multiple arms/events
        self.__record_ids = df_obj[self.__primary_key].unique().tolist()

        logging.info('Total number of instances - %s',
                     len(df_obj[self.__primary_key]))

        return True

    def export_records(self,
                       exp_format: str = 'csv',
                       record_ids: list[int | str] = None,
                       forms: list[str] = None,
                       events: list[str] = None,
                       filters: str = None) -> str | bool:
        """ Export records from the project.

        Args:
            exp_format (str, optional): Export format. Defaults to 'csv'.
            record_ids (list[int | str], optional): List of record IDs to be exported,
                                                    If not specified all records exported.
            forms (list[str], optional): List of forms to be included
            events (list[str], optional): List of events to be included
            filters (str, optional): String of logic text (e.g., [age] > 30) for filtering the data to be returned

        Returns:
            str | bool: List of records in JSON format string or False if an error occured
        """

        data = {
            'token': self.__token,
            'content': 'record',
            'action': 'export',
            'format': exp_format,
            'type': 'flat',
            'returnFormat': 'json'
        }

        # If set of record ids specified, export only those records.
        if record_ids:
            for i, record_id in enumerate(record_ids):
                data[f'records[{ i }]'] = record_id

        add_pk_field = False

        # If set of forms specified, export records only from those forms.
        if forms:
            for i, form in enumerate(forms):
                data[f'forms[{ i }]'] = form
            add_pk_field = True

        # If set of events specified, export records only for those events.
        if events:
            for i, event in enumerate(events):
                data[f'events[{ i }]'] = event
            add_pk_field = True

        # If any filters specified, export records only matching records.
        if filters:
            data['filterLogic'] = filters

        # If exporting only subset of forms or events, make sure to request the primary key field,
        # need it for importing data to the destination project
        if add_pk_field:
            data['fields[0]'] = self.__primary_key

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to export records')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        if not response.text.strip():
            return False

        return response.text

    def import_arms(self, arms: str) -> int | bool:
        """ Import project arms definitions in JSON format
            Delete all existing arms and import.

        Args:
            arms (str): Arms to be imported as JSON format string

        Returns:
            int | bool: Number of arms imported or False if an error occured
        """

        data = {
            'token': self.__token,
            'content': 'arm',
            'action': 'import',
            'format': 'json',
            'override': 1,
            'data': arms
        }

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to import arms')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        logging.info('Number of arms imported: %s', response.text)
        return int(response.text)

    def import_events(self, events: str) -> int | bool:
        """ Import project event definitions in JSON format
            Delete all existing events and import.

        Args:
            events (str): Events to be imported as JSON format string

        Returns:
            int | bool: Number of events imported or False if an error occured
        """

        data = {
            'token': self.__token,
            'content': 'event',
            'action': 'import',
            'format': 'json',
            'override': 1,
            'data': events
        }

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to import events')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        logging.info('Number of events imported: %s', response.text)
        return int(response.text)

    def import_data_dictionary(self, data_dict: str) -> int | bool:
        """ Import project data-dictionary in JSON format.

        Args:
            data_dict (str): Data-dictionary to be imported as JSON format string

        Returns:
            int | bool: Number of fields imported or False if an error occured
        """

        data = {
            'token': self.__token,
            'content': 'metadata',
            'format': 'json',
            'data': data_dict,
            'returnFormat': 'json'
        }

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to import data dictionary')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        logging.info('Number of fields imported: %s', response.text)
        return int(response.text)

    def import_form_event_mappings(self, form_event_map: str) -> int | bool:
        """ Import project instrument-event mappings in JSON format.

        Args:
            form_event_map (str): instrument-event mappings to be imported as JSON format string

        Returns:
            int | bool: Number of form-event mappings imported or False if an error occured
        """

        data = {
            'token': self.__token,
            'content': 'formEventMapping',
            'data': form_event_map,
            'format': 'json',
            'returnFormat': 'json'
        }

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to import form event mappings')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        logging.info('Number of form-event mappings imported: %s',
                     response.text)

        return int(response.text)

    def import_repeating_instruments(self, repeating_ins: str) -> int | bool:
        """ Import project repeating instrument definitions in JSON format.

        Args:
            repeating_ins (str): Repeating instrument to be imported in JSON format string

        Returns:
            int | bool: Number of repeating instruments imported or False if an error occured
        """

        data = {
            'token': self.__token,
            'content': 'repeatingFormsEvents',
            'data': repeating_ins,
            'format': 'json',
            'returnFormat': 'json'
        }

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to import form event mappings')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        logging.info('Number of repeating instruments/events imported: %s',
                     response.text)

        return int(response.text)

    def import_records(self,
                       values: str,
                       imp_format: str = 'csv') -> int | bool:
        """ Import records to the project.

        Args:
            values (str): List of records to be imported (in csv or json format string)
            imp_format (str, optional): Import formart. Defaults to 'csv'.

        Returns:
            int | bool: Number of records imported or False if an error occured
        """

        data = {
            'token': self.__token,
            'content': 'record',
            'action': 'import',
            'format': imp_format,
            'type': 'flat',
            'overwriteBehavior': 'normal',
            'forceAutoNumber': 'false',
            'data': values,
            'returnContent': 'count',
            'returnFormat': 'json'
        }

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to import records')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        num_records = json.loads(response.text)['count']

        return num_records

    def delete_records(self, record_ids: list[int | str]) -> int | bool:
        """ Delete records from the project.

        Args:
            record_ids (list[int  |  str]): List of record IDs to be deleted

        Returns:
            int | bool: Number of records deleted or False if an error occured
        """

        data = {
            'token': self.__token,
            'content': 'record',
            'action': 'delete',
            'returnFormat': 'json'
        }

        # Delete the specified records.
        if record_ids is not None:
            for i, record_id in enumerate(record_ids):
                data[f'records[{ i }]'] = record_id

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to delete records')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        return int(response.text)

    def delete_record(self,
                      record_id: str,
                      instrument: str = None,
                      event: str = None,
                      instance: int = None) -> int | bool:
        """ Delete a record from the project.

        Args:
            record_id (str): Record to be deleted
            instrument (str, optional): REDCap instrument name.
            event (str, optional): REDCap event name.
            instance (_type_, optional): REDCap repeating instance number.

        Returns:
            int | bool: Number of records deleted or False if an error occured
        """

        data = {
            'token': self.__token,
            'content': 'record',
            'action': 'delete',
            'returnFormat': 'json'
        }

        # Delete the specified record.
        data['records[0]'] = record_id
        if instrument:
            data['instrument'] = instrument
        if event:
            data['event'] = event
        if instance:
            data['repeat_instance'] = instance

        response = requests.post(self.__url, data=data)
        if response.status_code != HTTPStatus.OK:
            logging.error('Failed to delete records')
            logging.info('HTTP Status: %s %s : %s', str(response.status_code),
                         response.reason, response.text)
            return False

        return int(response.text)
