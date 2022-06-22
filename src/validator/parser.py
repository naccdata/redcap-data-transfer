""" Module for parsing validation rules """

import json
import logging

from json.decoder import JSONDecodeError
from typing import Any

from validator.rule import MaxValueRule
from validator.rule import NumericRangeRule
from validator.rule import Rule
from validator.variable import Variable


class Keys:
    FIELDS = 'fields'
    FLD_NAME = 'name'
    FLD_TYPE = 'type'
    FLD_LABEL = 'label'
    FLD_RULES = 'rules'
    RULE_CODE = 'code'
    RULE_MIN = 'min'
    RULE_MAX = 'max'


class Parser:
    """ Class to load the validation rules as python objects """

    rules_dir = './rules/'

    def __init__(self, rules_dir):
        self.rules_dir = rules_dir

    def parse_json(self, forms: list[str]) -> dict[str, Variable]:
        """ Populate variables dictionary from form definitions """

        logging.info('Loading data validation rules...')

        variables: dict[str, Variable] = {}
        for form in forms:
            form_def_file = self.rules_dir + form + '.json'
            try:
                with open(form_def_file, 'r', encoding='utf-8') as file_object:
                    form_def = json.load(file_object)
            except (FileNotFoundError, OSError, JSONDecodeError,
                    TypeError) as e:
                logging.error(
                    'Failed to load the form definition file - %s : %s',
                    form_def_file, e)
                logging.warning('Skipping validations for the form - %s', form)
                continue

            if Keys.FIELDS in form_def:
                field_defs = form_def[Keys.FIELDS]
                for field_def in field_defs:
                    if self.validate_field_def(field_def):
                        field_name = field_def[Keys.FLD_NAME]
                        rule_defs = field_def[Keys.FLD_RULES]
                        rules = []
                        for rule_def in rule_defs:
                            if self.validate_rule_def(rule_def):
                                rule_obj = self.get_rule_object(rule_def)
                                if rule_obj is not None:
                                    rules.append(rule_obj)
                        if len(rules) != 0:
                            variable = Variable(field_name,
                                                field_def[Keys.FLD_TYPE],
                                                field_def[Keys.FLD_LABEL],
                                                rules)
                            variables[field_name] = variable

        return variables

    def validate_field_def(self, field_def: dict[str, Any]) -> bool:
        """ Validate the field definition """

        if Keys.FLD_NAME not in field_def:
            return False
        if Keys.FLD_TYPE not in field_def:
            return False
        if Keys.FLD_LABEL not in field_def:
            return False
        if Keys.FLD_RULES not in field_def:
            return False

        return True

    def validate_rule_def(self, rule_def: dict[str, Any]) -> bool:
        """ Validate the rule definition """

        if Keys.RULE_CODE not in rule_def:
            return False

        if rule_def[Keys.RULE_CODE] == 'NUMRANGE':
            if Keys.RULE_MAX not in rule_def:
                return False
            if Keys.RULE_MIN not in rule_def:
                return False

        if rule_def[Keys.RULE_CODE] == 'MAXVAL':
            if Keys.RULE_MAX not in rule_def:
                return False

        return True

    def get_rule_object(self, rule_def: dict[str, Any]) -> Rule | None:
        """ Populate respective rule object according to rule code """

        rule = None
        code = rule_def[Keys.RULE_CODE]
        if code == 'NUMRANGE':
            rule = NumericRangeRule(code, rule_def[Keys.RULE_MIN],
                                    rule_def[Keys.RULE_MAX])

        if code == 'MAXVAL':
            rule = MaxValueRule(code, rule_def[Keys.RULE_MAX])

        return rule
