import sys

from configs import Configs
from redcap_api.redcap_connection import REDCapConnection
from redcap_api.data_handler import DataHandler

# programm entry
def main():
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    if sys.argv[1] in ["-h", "--help"]:
        usage()
        sys.exit(0)

    if len(sys.argv) > 1:
        conf_file = sys.argv[1]

    # Load and validate the configurations
    Configs.load_configs(conf_file)

    if Configs.configs['src_api_token'] == Configs.configs['dest_api_token']:
        print('Error: source and destination projects cannot be the same, please check API tokens')
        exit(0)

    # Initialize source and destination REDCap connections
    src_project = REDCapConnection(Configs.configs['src_api_token'], Configs.configs['src_api_url'])
    dest_project = REDCapConnection(Configs.configs['dest_api_token'], Configs.configs['dest_api_url'])

    data_handler = DataHandler(src_project, dest_project)

    # If source and destination project settings matches, move/copy the records from source project to destination project
    if data_handler.compare_project_settings():
        data_handler.move_data()
    
def usage():
    print('Usage: python3 data_validator.py <configuration file path>')


if __name__ == "__main__":
    main()
