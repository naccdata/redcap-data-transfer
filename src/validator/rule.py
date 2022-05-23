""" Rules module """

from abc import abstractmethod


class Rule:
    """ Abstract class to represent a validation rule """

    @abstractmethod
    def __init__(self, code: str) -> None:
        self.code = code
        self.error_msg = ''

    def apply(self, var_name, var_value: any) -> bool:
        """ Apply the rule on the specified variable """
        return False

    def set_error(self, error_msg: str):
        self.error_msg = error_msg

    def get_error(self) -> str:
        return self.error_msg


class NumericRangeRule(Rule):
    """ Class to represent a range check for numeric variable """

    def __init__(self, code: str, min_val: float, max_val: float) -> None:
        super().__init__(code)
        self.min_val = min_val
        self.max_val = max_val

    def apply(self, var_name, var_value: float) -> bool:
        """ Apply the rule on the specified variable value """

        super().set_error('')

        if self.min_val <= var_value <= self.max_val:
            return True

        super().set_error(
            f'Range check failed for the field "{var_name}": current value - {var_value}, expected range - [{self.min_val} - {self.max_val}]'
        )
        return False
