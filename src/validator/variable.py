""" Module for form variables """

from validator.rule import Rule


class Variable:
    """ Class to represent a form variable """

    def __init__(self,
                 name: str,
                 data_type: str,
                 label: str,
                 rules: list[Rule] = None):
        self.name = name  # Variable name (unique across the forms in a project)
        self.data_type = data_type  # Variable data type
        self.label = label  # Variable label (or question) in the form
        self.rules = rules  # List of quality rules defined for this variable
        self.errors: list[str] = []  # List of validation errors

    def get_errors_list(self) -> list[str]:
        """ Returns the list of latest validation erros """

        return self.errors

    def validate(self, value) -> bool:
        """ Evaluate the passed value aginst the defined rules """

        self.errors.clear()
        valid = True
        for rule in self.rules:
            if not rule.apply(self.name, value):
                self.errors.append(rule.get_error())
                valid = False

        return valid

    def cast_value(self, value: str) -> any:
        """ Cast the input value to appropriate data type """
        # TODO - need to replace this by more appropriate way of handling data types

        if not value:
            return None

        try:
            if self.data_type == 'INT':
                return int(value)

            if self.data_type == 'FLOAT':
                return float(value)

            if self.data_type == 'BOOL':
                return bool(value)
        except ValueError:
            return value
            
        return value
