""" Module for performing data quality checks """

import logging

from typing import Tuple

from validator.parser import Parser
from validator.variable import Variable


class QualityCheck:
    """ Class to run the validation rules """

    def __init__(self,
                 primary_key: str,
                 log_file: str,
                 rules_dir: str,
                 forms: list[str] = None):
        # Dictionary of variables defined in the project by variable name
        self.variables: dict[str, Variable] = {}
        # Validation error log to record any mismatches
        self.logger: logging.Logger = self.__setup_logger(log_file)
        # Primary key field of the project
        self.primary_key: str = primary_key

        self.__load_rules(rules_dir, forms)

    def __setup_logger(self, log_file: str) -> logging.Logger:
        """ Create a custom logger with file handler """

        logger = logging.getLogger(__name__)

        # Create file handler
        f_handler = logging.FileHandler(log_file)
        f_handler.setLevel(logging.INFO)

        # Create formatter and add it to handlers
        f_format = logging.Formatter(
            fmt='%(asctime)s [%(levelname)s] - %(message)s',
            datefmt='%m-%d-%y %H:%M:%S')
        f_handler.setFormatter(f_format)

        # Add handlers to the logger
        logger.addHandler(f_handler)

        return logger

    def __load_rules(self, rules_dir: str, forms: list[str] = None):
        """ Load the set of variables and rules defined for the project """

        # This function depends on how rules are represented,
        # Load few test rules from a json file for now

        if forms:
            parser = Parser(rules_dir)
            self.variables = parser.parse_json(forms)

    def check_record(self, record: dict[str,
                                        str]) -> Tuple[bool, dict[str, str]]:
        """ Evaluate the record against the defined rules """

        passed = True
        errors = {}

        # Iterate through the fields in the input record,
        # and validates the quality rules if there's any defined
        for key in record:
            if key in self.variables:
                variable: Variable = self.variables[key]
                if variable:
                    # TODO - need a better way for hadling data types
                    # using casting for now
                    value = variable.cast_value(record[key])
                    if (value is not None) and (not variable.validate(value)):
                        errors[key] = '\n'.join(variable.get_errors_list())
                        passed = False

        # Log the errors if validation failed
        if not passed:
            self.logger.error(
                'Validation failed for the record %s = %s, list of errors:',
                self.primary_key, record[self.primary_key])
            for value in errors.values():
                self.logger.info(value)
            return False, errors

        return True, errors
