#!/usr/bin/env python3

import sys
import logging

from configs import Configs
from redcap_api.redcap_connection import REDCapConnection
from redcap_api.data_handler import DataHandler

# programm entry
def main():
    if len(sys.argv) < 3:
        usage()
        exit(1)

    if sys.argv[1] in ["-h", "--help"]:
        usage()
        exit(0)

    conf_file = sys.argv[1]
    log_file = sys.argv[2]

    Configs.setup_logger(log_file)

    # Load and validate the configurations
    Configs.load_configs(conf_file)

    if Configs.configs['src_api_token'] == Configs.configs['dest_api_token']:
        logging.critical('Source and destination projects cannot be the same, please check the API tokens in configurations file')
        exit(1)

    logging.info('==================== STARTING data transfer ====================')

    # Initialize source and destination REDCap connections
    src_project = REDCapConnection(Configs.configs['src_api_token'], Configs.configs['src_api_url'])
    dest_project = REDCapConnection(Configs.configs['dest_api_token'], Configs.configs['dest_api_url'])

    data_handler = DataHandler(src_project, dest_project)

    # If source and destination project settings matches, move/copy the records from source project to destination project
    if data_handler.compare_project_settings():
        data_handler.move_data()
    
    logging.info('==================== ENDING data transfer ====================')


def usage():
    print('Usage: python3 data_validator.py <configuration file path> <log file path>')


if __name__ == "__main__":
    main()
