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
        except json.decoder.JSONDecodeError as err:
            err_msg = 'Error in loading configuration file: '+conf_file_path+', '+err.msg+' line {} col {}.'
            sys.exit(err_msg.format(err.lineno, err.colno))
