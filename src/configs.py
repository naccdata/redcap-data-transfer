import json
import logging
import sys

from json.decoder import JSONDecodeError


class Configs:
    """ This class stores configuration variables """

    configs = {}

    @classmethod
    def load_configs(cls, conf_file_path: str):
        """ Loads the specified configuration file """

        try:
            with open(conf_file_path, 'r', encoding='utf-8') as file_object:
                Configs.configs = json.load(file_object)
        except (FileNotFoundError, OSError, JSONDecodeError, TypeError):
            logging.critical('Failed to load the configuration file',
                             exc_info=True)
            sys.exit(1)

        if 'src_api_token' not in Configs.configs:
            logging.critical(
                'Missing src_api_token, please check the configurations file')
            sys.exit(1)

        if 'dest_api_token' not in Configs.configs:
            logging.critical(
                'Missing dest_api_token, please check the configurations file')
            sys.exit(1)

        if 'src_api_url' not in Configs.configs:
            logging.critical(
                'Missing src_api_url, please check the configurations file')
            sys.exit(1)

        if 'dest_api_url' not in Configs.configs:
            logging.critical(
                'Missing dest_api_url, please check the configurations file')
            sys.exit(1)

        #set default value to 100
        if 'batch_size' not in Configs.configs:
            Configs.configs['batch_size'] = 100

        #set default value to copy only
        if 'move_records' not in Configs.configs:
            Configs.configs['move_records'] = 0
            