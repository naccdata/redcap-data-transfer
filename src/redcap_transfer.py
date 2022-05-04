#!/usr/bin/env python3

import argparse
import logging
import sys

from data_handler import DataHandler
from params import Params
from redcap_connection import REDCapConnection


# programm entry
def main():
    # Get the configguration file path
    parser = argparse.ArgumentParser()
    parser.add_argument('conf_file',
                        type=str,
                        help='Configurations file path',
                        nargs='?',
                        default=None)
    args = parser.parse_args()

    # Set up logger
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] - %(message)s',
                        datefmt='%m-%d-%y %H:%M:%S')

    # Load and validate the project parameters
    if not Params.load_parameters(args.conf_file):
        sys.exit(1)

    logging.info('================= STARTING data transfer ==================')

    # Initialize source and destination REDCap connections
    src_project = REDCapConnection(Params.SRC_API_TOKEN, Params.SRC_API_URL)
    dest_project = REDCapConnection(Params.DEST_API_TOKEN, Params.DEST_API_URL)

    data_handler = DataHandler(src_project, dest_project)

    # If source and destination project settings matches,
    # move/copy the records from source project to destination project
    if data_handler.compare_project_settings():
        data_handler.move_data()

    logging.info('================== ENDING data transfer ===================')


if __name__ == '__main__':
    main()
