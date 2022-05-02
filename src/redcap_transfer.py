#!/usr/bin/env python3

import argparse
import sys
import logging

from configs import Configs
from data_handler import DataHandler
from redcap_connection import REDCapConnection


# programm entry
def main():
    # Get the configguration file path
    parser = argparse.ArgumentParser()
    parser.add_argument('conf_file', type=str, help='Configurations file path')
    args = parser.parse_args()

    #Set up logger configurations
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] - %(message)s',
                        datefmt='%m-%d-%y %H:%M:%S')

    # Load and validate the configurations
    if not Configs.load_configs(args.conf_file):
        sys.exit(1)

    logging.info('================= STARTING data transfer ==================')

    # Initialize source and destination REDCap connections
    src_project = REDCapConnection(Configs.configs['src_api_token'],
                                   Configs.configs['src_api_url'])
    dest_project = REDCapConnection(Configs.configs['dest_api_token'],
                                    Configs.configs['dest_api_url'])

    data_handler = DataHandler(src_project, dest_project)

    # If source and destination project settings matches,
    # move/copy the records from source project to destination project
    if data_handler.compare_project_settings():
        data_handler.move_data()

    logging.info('================== ENDING data transfer ===================')


def usage():
    print('Usage: python3 redcap_transfer.py <configuration file path>')


if __name__ == '__main__':
    main()
