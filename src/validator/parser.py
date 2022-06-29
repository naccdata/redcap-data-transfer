""" Module for parsing rule definition schemas """

import json
import logging

from json.decoder import JSONDecodeError
from typing import Mapping, Tuple


class Parser:
    """ Class to load the validation rules as python objects """

    rules_dir = './rules/'

    def __init__(self, rules_dir: str):
        # Location where rule definitions are stored
        self.rules_dir = rules_dir

    def load_schema_from_json(
            self,
            forms: list[str]) -> Tuple[dict[str, Mapping[str, object]], bool]:
        """ Load rule schema from JSON form definitions """

        logging.info(
            'Loading data validation schema from JSON rule definitions ...')

        full_schema: dict[str, Mapping[str, object]] = {}
        found_all = True
        for form in forms:
            form_def_file = self.rules_dir + form + '.json'
            try:
                with open(form_def_file, 'r', encoding='utf-8') as file_object:
                    form_def = json.load(file_object)
            except (FileNotFoundError, OSError, JSONDecodeError,
                    TypeError) as e:
                logging.warning(
                    'Failed to load the form definition file - %s : %s',
                    form_def_file, e)
                found_all = False
                continue

            if form_def:
                # If there's any duplicate keys(i.e. variable names) across forms,
                # they will be replaced with the latest definitions.
                # It is assumed all variable names are unique within a project
                full_schema.update(form_def)

        return full_schema, found_all
