""" QualityCheck module """

import logging

from validator.variable import Variable
from validator.rule import NumericRangeRule


class QualityCheck:
    """ Class to run the validation rules """

    def __init__(self, primary_key: str, log_file: str):
        # Dictionary of variables defined in the project by variable name
        self.variables: dict[Variable] = {}
        # Validation error log to record any mismatches
        self.logger: logging.Logger = self.__setup_logger(log_file)
        # Primary key field of the project
        self.primary_key: str = primary_key

        self.__load_rules()

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

    def __load_rules(self) -> bool:
        """ Load the set of variables and rules defined for the project """

        # This function depends on how rules are represented,
        # hard-code few test rule for now
        weight_rules = []
        weight_range_rule = NumericRangeRule('NUMRANGE', 50, 90)
        weight_rules.append(weight_range_rule)
        var_weight = Variable('weight', 'INT', 'Weight (lb)', weight_rules)
        self.variables['weight'] = var_weight

        height_rules = []
        height_range_rule = NumericRangeRule('NUMRANGE', 140, 180)
        height_rules.append(height_range_rule)
        var_height = Variable('height', 'INT', 'Weight (lb)', height_rules)
        self.variables['height'] = var_height

    def check_record(self, record: dict[str]) -> bool:
        """ Evaluate the record against the defined rules """

        passed = True
        errors = []
        
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
                        errors.extend(variable.get_errors_list())
                        passed = False

        # Log the errors if validation failed
        if not passed:
            self.logger.error(
                'Validation failed for the record %s = %s, list of errors:',
                self.primary_key, record[self.primary_key])
            for error in errors:
                self.logger.info(error)
            return False

        return True
