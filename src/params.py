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
    LOG_FILE_PATH: str = './logs/'
    LOG_FILE_PREFIX: str = 'validation-errors-'
    RULES_DIR: str = './rules/'

    # Optional parameters
    extra_params = {}

    @classmethod
    def load_parameters(cls, conf_file_path: str = None) -> bool:
        """ Load project parameters to class attributes """

        # Load environment variables
        try:
            Params.SRC_API_TOKEN = decouple.config('SRC_API_TOKEN')
            Params.DEST_API_TOKEN = decouple.config('DEST_API_TOKEN')
            Params.SRC_API_URL = decouple.config('SRC_API_URL')
            Params.DEST_API_URL = decouple.config('DEST_API_URL')
            Params.BATCH_SIZE = decouple.config('BATCH_SIZE',
                                                default=100,
                                                cast=int)
            Params.MOVE_RECORDS = decouple.config('MOVE_RECORDS',
                                                  default=0,
                                                  cast=int)
            Params.LOG_FILE_PATH = decouple.config('LOG_FILE_PATH',
                                                   default='./logs/')
            Params.LOG_FILE_PREFIX = decouple.config(
                'LOG_FILE_PREFIX', default='validation-errors-')
            Params.RULES_DIR = decouple.config('RULES_DIR', default='./rules')

            if not conf_file_path:
                conf_file_path = decouple.config('CONF_FILE_PATH',
                                                 default=None)
        except decouple.UndefinedValueError as e:
            logging.critical('Failed to load required parameters: %s', e)
            return False

        if Params.SRC_API_TOKEN == Params.DEST_API_TOKEN:
            logging.critical(
                'Source and destination projects cannot be the same, '
                'please check the API tokens')
            return False

        # Load the optional configuration file
        if conf_file_path:
            try:
                with open(conf_file_path, 'r',
                          encoding='utf-8') as file_object:
                    Params.extra_params = json.load(file_object)
            except (FileNotFoundError, OSError, JSONDecodeError,
                    TypeError) as e:
                logging.critical('Failed to load the configuration file: %s',
                                 e)
                return False

        return True
