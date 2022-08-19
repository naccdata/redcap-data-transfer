""" REDCapDatastore module """

import io
import logging
import pandas as pd

from redcap_api.redcap_connection import REDCapConnection, REDCapKeys
from validator.datastore import Datastore


class REDCapDatastore(Datastore):
    """ Class to retrieve longitudinal data from REDCap warehouse """

    def __init__(self,
                 redcap_con: REDCapConnection,
                 forms: list[str] = None,
                 events: list[str] = None):
        """

        Args:
            redcap_con (REDCapConnection): REDCap project to retrieve records.
            forms (list[str], optional): List of form names to be included.
            events (list[str], optional): List of events to be included.
        """

        self.__redcap_con: REDCapConnection = redcap_con
        self.__forms: list[str] = forms
        self.__events: list[str] = events

    def get_previous_instance(
            self, orderby: str,
            current_ins: dict[str, str]) -> dict[str, str] | bool:
        """ Overriding the abstract method, 
        get the previous instance of the specified record

        Args:
            orderby (str): Variable name that instances are sorted by
            current_ins (dict[str, str]): Instance currently being validated

        Returns:
            dict[str, str]: Previous instance or False if no instance found
        """

        # Skip the checks if this is first visit
        if not current_ins[REDCapKeys.RDCP_RPT_INSTN]:
            return False

        record_id = current_ins[self.__redcap_con.primary_key]
        curr_ob_fld_val = current_ins[orderby]
        filter_str = f"[{orderby}] < '{curr_ob_fld_val}'"
        prev_records = self.__redcap_con.export_records('csv', [record_id],
                                                        self.__forms,
                                                        self.__events,
                                                        filters=filter_str)
        if prev_records:
            # Read the response into a pandas data frame
            df = pd.read_csv(io.StringIO(prev_records), keep_default_na=False)
            df = df.sort_values(orderby, ascending=False)
            latest_rec = df.head(1)
            return latest_rec.to_dict('records')[0]
        else:
            logging.info('No previous records found for %s=%s and %s',
                         self.__redcap_con.primary_key, record_id, filter_str)
            return False
