""" Module for parsing rule definition schemas """

import json
import logging
import yaml

from json.decoder import JSONDecodeError
from typing import Mapping, Tuple
from yaml.loader import SafeLoader


class Parser:
    """ Class to load the validation rules as python objects """

    def __init__(self, rules_dir: str, rules_type: str = 'yaml'):
        """_summary_

        Args:
            Args:
            rules_dir (str): Location where rule definitions are stored
            type (str, optional): Type of the parser (json or yaml). Defaults to 'yaml'.
        """

        self.__rules_dir = rules_dir
        self.__rules_type = rules_type

    def load_validation_schema(
            self,
            forms: list[str]) -> Tuple[dict[str, Mapping[str, object]], bool]:
        """ Load rule schema from the form definition files.

        Args:
            forms (list[str]): List of form names to load the rule definitions

        Returns:
            dict[str, Mapping[str, object]: Schema object created from rule definitions,
            bool: True if all form definitions successfully parsed
        """

        logging.info(
            'Loading data validation schema from rule definitions ...')

        full_schema: dict[str, Mapping[str, object]] = {}
        found_all = True
        for form in forms:
            form_def = self.__load_file(form)

            if not form_def:
                found_all = False
                continue

            # If there's any duplicate keys(i.e. variable names) across forms,
            # they will be replaced with the latest definitions.
            # It is assumed all variable names are unique within a project
            full_schema.update(form_def)

        return full_schema, found_all

    def __load_file(self,
                    form_name: str) -> dict[str, Mapping[str, object]] | None:
        """ Load the rules schema for the specified form

        Args:
            form_name (str): Form name to load the rules

        Returns:
            dict[str, Mapping[str, object]] | None: Schema object loaded from definition file
        """

        form_def = None
        form_def_file = self.__rules_dir + form_name + '.' + self.__rules_type

        try:
            with open(form_def_file, 'r', encoding='utf-8') as file_object:
                if self.__rules_type == 'json':
                    form_def = json.load(file_object)
                else:
                    form_def = yaml.load(file_object, Loader=SafeLoader)
        except (FileNotFoundError, OSError, JSONDecodeError, yaml.YAMLError,
                TypeError) as e:
            logging.warning(
                'Failed to load the form definition file - %s : %s',
                form_def_file, e)

        return form_def
