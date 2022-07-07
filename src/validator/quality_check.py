""" Module for performing data quality checks """

import logging
import cerberus.schema

from typing import Mapping, Tuple

from validator.parser import Parser
from validator.rule_validator import CustomErrorHandler, RuleValidator


class QualityCheckException(Exception):
    pass


class QualityCheck:
    """ Class to run the validation rules """

    def __init__(self,
                 primary_key: str,
                 rules_dir: str,
                 forms: list[str],
                 strict: bool = True):
        """

        Args:
            primary_key (str): Primary key field of the project
            rules_dir (str): Location where rule definitions are stored
            forms (list[str]): List of form names to load the rule definitions
            strict (bool, optional): Validation mode. Defaults to True. 
                                     If False, unknown forms/fields are skipped from validation
        """

        self.primary_key: str = primary_key
        self.strict = strict
        # Schema of validation rules defined in the project by variable name
        self.schema: dict[str, Mapping[str, object]] = {}
        # Validator object for rule evaluation
        self.validator: RuleValidator = None

        self.__load_rules(rules_dir, forms)

    def __load_rules(self, rules_dir: str, forms: list[str]):
        """ Load the set of validation rules defined for the variables

        Args:
            rules_dir (str): Location where rule definitions are stored
            forms (list[str]): List of form names to load the rule definitions

        Raises:
            QualityCheckException: If there is an schema error 
                                   or if some form definitons are missing in strict mode
        """

        if forms:
            parser = Parser(rules_dir)
            self.schema, found_all = parser.load_schema_from_json(forms)
            if not found_all:
                if self.strict:
                    raise QualityCheckException(
                        f'Missing validation rule definitions, please check {rules_dir} directory'
                    )
                else:
                    logging.warning(
                        'Skipping validation rules for the form(s) listed above'
                    )
        elif self.strict:
            raise QualityCheckException(
                'Failed to load validation rules, forms list is empty')
        else:
            logging.warning(
                'No forms are specified to load the rule definitions, skipping validation rules'
            )

        try:
            self.validator = RuleValidator(self.schema,
                                           allow_unknown=not self.strict,
                                           error_handler=CustomErrorHandler(
                                               self.schema))
        except cerberus.schema.SchemaError as e:
            raise QualityCheckException(f'Schema Error - {e}')

    def check_record_cerberus(
            self, record: dict[str, str]) -> Tuple[bool, dict[str, list[str]]]:
        """ Evaluate the record against the defined rules using cerberus.

        Args:
            record (dict[str, str]): Record to be validated, dict[field, value]

        Returns:
            bool: True if the record satisfied all rules
            dict[str, list[str]: List of validation errors by variable (if any)
        """

        # All the fields in the input record represented as string values,
        # cast the fields to appropriate data types according to the schema before validation
        record = self.validator.cast_record(record)
        # Validate the record against the defined schema
        passed = self.validator.validate(record, normalize=False)
        errors = self.validator.errors

        return passed, errors
