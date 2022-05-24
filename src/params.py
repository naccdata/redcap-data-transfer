""" Module to load project configurations """

import decouple
import json
import logging

from json.decoder import JSONDecodeError


class Params:
    """ This class stores the project parameters/environment variables """

    # Required parameters
    SRC_API_TOKEN: str = ''
    DEST_API_TOKEN: str = ''
    SRC_API_URL: str = ''
    DEST_API_URL: str = ''

    # Parameters with default values
    BATCH_SIZE: int = 100
    MOVE_RECORDS: int = 0
    LOG_FILE_DIR: str = './logs/'
    LOG_FILE_PREFIX: str = 'validation-errors-'
    RULES_DIR: str = './rules/'

    # Optional parameters
    CONF_FILE_PATH = None
    extra_params = {}

    @classmethod
    def load_parameters(cls) -> bool:
        """ Load project parameters to class attributes """

        # Load environment variables
        try:
            cls.SRC_API_TOKEN = decouple.config('SRC_API_TOKEN')
            cls.DEST_API_TOKEN = decouple.config('DEST_API_TOKEN')
            cls.SRC_API_URL = decouple.config('SRC_API_URL')
            cls.DEST_API_URL = decouple.config('DEST_API_URL')
            cls.BATCH_SIZE = decouple.config('BATCH_SIZE',
                                             default=100,
                                             cast=int)
            cls.MOVE_RECORDS = decouple.config('MOVE_RECORDS',
                                               default=0,
                                               cast=int)
            cls.LOG_FILE_DIR = decouple.config('LOG_FILE_DIR',
                                                default='./logs/')
            cls.LOG_FILE_PREFIX = decouple.config('LOG_FILE_PREFIX',
                                                  default='validation-errors-')
            cls.RULES_DIR = decouple.config('RULES_DIR', default='./rules/')
            cls.CONF_FILE_PATH = decouple.config('CONF_FILE_PATH',
                                                 default=None)
        except decouple.UndefinedValueError as e:
            logging.critical('Failed to load required parameters: %s', e)
            return False

        if cls.SRC_API_TOKEN == cls.DEST_API_TOKEN:
            logging.critical(
                'Source and destination projects cannot be the same, '
                'please check the API tokens')
            return False

        # Load the optional configuration file
        if cls.CONF_FILE_PATH:
            try:
                with open(cls.CONF_FILE_PATH, 'r',
                          encoding='utf-8') as file_object:
                    cls.extra_params = json.load(file_object)
            except (FileNotFoundError, OSError, JSONDecodeError,
                    TypeError) as e:
                logging.critical('Failed to load the configuration file: %s',
                                 e)
                return False

        return True
