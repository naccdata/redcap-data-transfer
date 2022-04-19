import sys
import json

# This class stores configuration variables

class Configs:
    configs = None

    @staticmethod
    def load_configs(conf_file_path):
        try:
            file_object = open(conf_file_path)
        except:
            sys.exit('Failed to open configuration file: '+conf_file_path)

        try:
            Configs.configs = json.load(file_object)

            if 'src_api_token' not in Configs.configs:
                print('Error: Missing src_api_token, please check the configurations file')
                exit(1)

            if 'dest_api_token' not in Configs.configs:
                print('Error: Missing dest_api_token, please check the configurations file')
                exit(1)
            
            if 'src_api_url' not in Configs.configs:
                print('Error: Missing src_api_url, please check the configurations file')
                exit(1)

            if 'dest_api_url' not in Configs.configs:
                print('Error: Missing dest_api_url, please check the configurations file')
                exit(1)

            if 'batch_size' not in Configs.configs:
                Configs.configs['batch_size'] = 100 #set default value to 100
            
            if 'move_records' not in Configs.configs:
                Configs.configs['move_records'] = 0 #set default value to copy only

        except json.decoder.JSONDecodeError as err:
            err_msg = 'Error in loading configuration file: '+conf_file_path+', '+err.msg+' line {} col {}.'
            sys.exit(err_msg.format(err.lineno, err.colno))
