import json
import logging

# This class stores configuration variables

class Configs:
    configs = None

    @staticmethod
    def load_configs(conf_file_path):
        try:
            file_object = open(conf_file_path)
            Configs.configs = json.load(file_object)

            if 'src_api_token' not in Configs.configs:
                logging.critical('Missing src_api_token, please check the configurations file')
                exit(1)

            if 'dest_api_token' not in Configs.configs:
                logging.critical('Missing dest_api_token, please check the configurations file')
                exit(1)
            
            if 'src_api_url' not in Configs.configs:
                logging.critical('Missing src_api_url, please check the configurations file')
                exit(1)

            if 'dest_api_url' not in Configs.configs:
                logging.critical('Missing dest_api_url, please check the configurations file')
                exit(1)

            if 'batch_size' not in Configs.configs:
                Configs.configs['batch_size'] = 100 #set default value to 100
            
            if 'move_records' not in Configs.configs:
                Configs.configs['move_records'] = 0 #set default value to copy only
        except:
            logging.critical('Exception occurred', exc_info=True)
            exit(1)


    @staticmethod
    def setup_logger(log_file_path):
        try:
            logging.basicConfig(filename=log_file_path,
                                level=logging.INFO,
                                format='%(asctime)s [%(levelname)s] - %(message)s',
                                datefmt='%m-%d-%y %H:%M:%S')
        except:
            logging.critical('Exception occurred', exc_info=True)
            exit(1)