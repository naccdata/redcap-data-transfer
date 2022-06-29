""" Module for defining validation rules """

import cerberus.schema

from typing import Mapping
from cerberus.errors import BasicErrorHandler
from cerberus.validator import Validator


class SchemaDefs:
    """ Class to store schema attribute labels and default values """

    TYPE = 'type'
    IF = 'if'
    THEN = 'then'
    META = 'meta'
    ERRMSG = 'errmsg'


class CustomErrorHandler(BasicErrorHandler):
    """ Class to provide custom error messages  """

    def __init__(self, schema=None, tree=None):
        super().__init__()
        self.custom_schema = schema

    def _format_message(self, field, error):
        """ Display custom error message.
            Use the 'meta' tag in the schema to specify a custom error message
            e.g. "meta": {"errmsg": "<custom message>"}
        """

        if self.custom_schema:
            error_msg = self.custom_schema[field].get(SchemaDefs.META, {}).get(
                SchemaDefs.ERRMSG, '')
            if error_msg:
                return field + ': ' + error_msg

        return super()._format_message(field, error)


class RuleValidator(Validator):
    """ RuleValidator class to extend cerberus.Validator """

    def __init__(self, schema: Mapping, *args, **kwargs):
        super().__init__(schema, *args, **kwargs)
        self._dtypes: dict[str, str] = self.__populate_data_types()

    def __populate_data_types(self) -> dict[str, str] | None:
        """ Convert cerberus data types to python data types.
            Populates a field->type mapping for each field in the schema
        """

        if not self.schema:
            return None

        data_types = {}
        for key, configs in self.schema.items():
            if SchemaDefs.TYPE in configs:
                if configs[SchemaDefs.TYPE] == 'integer':
                    data_types[key] = 'int'
                elif configs[SchemaDefs.TYPE] == 'string':
                    data_types[key] = 'str'
                elif configs[SchemaDefs.TYPE] == 'float':
                    data_types[key] = 'float'
                elif configs[SchemaDefs.TYPE] == 'boolean':
                    data_types[key] = 'bool'
                elif configs[SchemaDefs.TYPE] == 'date':
                    data_types[key] = 'datetime.date'
                elif configs[SchemaDefs.TYPE] == 'datetime':
                    data_types[key] = 'datetime.datetime'

        return data_types

    @property
    def dtypes(self):
        """ The dtype property """
        return self._dtypes

    def cast_record(self, record: dict[str, str]) -> dict[str, object]:
        """ Cast the fields in the record to appropriate data types """

        if not self._dtypes:
            return record

        for key, value in record.items():
            # Set empty fields to None (to trigger nullable validations),
            # otherwise data type validation is triggered.
            # Don't remove empty fields from the record, if removed, any
            # validation rules defined for that field will not be triggered.
            if not value:
                record[key] = None
                continue

            try:
                if key in self._dtypes:
                    if self._dtypes[key] == 'int':
                        record[key] = int(value)
                    elif self._dtypes[key] == 'float':
                        record[key] = float(value)
                    elif self._dtypes[key] == 'bool':
                        record[key] = bool(value)
            except ValueError:
                record[key] = value

        return record

    def _validate_filled(self, filled: bool, field: str, value: object):
        """
            Custom method to check whether the 'filled' rule is met.
            This is different from 'nullable' rule
            The rule's arguments are validated against this schema:
                {'type': 'boolean'}
        """

        if not filled and value:
            self._error(field, 'must be empty')
        elif filled and not value:
            self._error(field, 'cannot be empty')

    def _validate_constraints(self, constraints: list[Mapping], field: str,
                              value: object):
        """ Validate the list of constraints specified for a field.
            The rule's arguments are validated against this schema:
                {'type': 'list'}
        """

        # Evaluate each constraint in the list individually,
        # validation fails if any of the constraints fails.
        for constraint in constraints:
            try:
                # Extract conditions for dependent fields
                dependent_conds = constraint[SchemaDefs.IF]
                # Extract conditions for self (i.e. field being evaluated)
                self_conds = constraint[SchemaDefs.THEN]
            except KeyError as e:
                #TODO - move schema validation to Parser class
                raise cerberus.schema.SchemaError(
                    f'Missing required attribute {e} in constraint {str(constraint)} for field \'{field}\''
                )

            valid = True
            # Check whether all dependency conditions are valid (evaluated as logical AND operation)
            for dep_field, conds in dependent_conds.items():
                subschema = {dep_field: conds}
                temp_validator = RuleValidator(
                    subschema,
                    allow_unknown=True,
                    error_handler=CustomErrorHandler(subschema))
                if not temp_validator.validate(self.document):
                    valid = False
                    break

            # If dependencies satisfied validate the conditions for self
            if valid:
                subschema = {field: self_conds}
                temp_validator = RuleValidator(
                    subschema, error_handler=CustomErrorHandler(subschema))
                temp_validator.validate({field: value})
                errors = temp_validator.errors.items()
                for error in errors:
                    self._error(field, f'{str(error)} for {dependent_conds}')
