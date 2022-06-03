#!/usr/bin/env python3

import logging
import sys

from datetime import datetime as dt

from data_handler import DataHandler
from params import Params
from redcap_connection import REDCapConnection, REDCapConnectionException


# programm entry
def main():
    # Set up logger
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] - %(message)s',
                        datefmt='%m-%d-%y %H:%M:%S')

    # Load and validate the project parameters
    if not Params.load_parameters():
        sys.exit(1)

    # Initialize source and destination REDCap connections
    try:
        src_project = REDCapConnection(Params.SRC_API_TOKEN,
                                       Params.SRC_API_URL)
    except REDCapConnectionException:
        logging.critical(
            'Error occurred while connecting to the source REDCap project')
        sys.exit(1)

    try:
        dest_project = REDCapConnection(Params.DEST_API_TOKEN,
                                        Params.DEST_API_URL)
    except REDCapConnectionException:
        logging.critical(
            'Error occurred while connecting to the destination REDCap project'
        )
        sys.exit(1)

    logging.info('Source Project: %s', src_project.get_project_title())
    logging.info('Destination Project: %s', dest_project.get_project_title())

    forms = None
    if 'forms' in Params.extra_params and \
        Params.extra_params['forms'].strip():
        forms = [x.strip() for x in Params.extra_params['forms'].split(',')]

    events = None
    if 'events' in Params.extra_params and \
        Params.extra_params['events'].strip():
        events = [x.strip() for x in Params.extra_params['events'].split(',')]

    qc_err_form = 'data_quality_check_errors'
    if 'errors_form' in Params.extra_params:
        qc_err_form = Params.extra_params['errors_form']

    data_handler = DataHandler(src_project, dest_project, forms, events,
                               qc_err_form)

    # Check whether source and destination project settings matches,
    logging.info('Comparing source and destination project compatibility...')
    if data_handler.compare_project_settings():
        current_time = dt.now()
        validation_error_log = Params.LOG_FILE_DIR + Params.LOG_FILE_PREFIX
        validation_error_log += current_time.strftime('%m%d%y-%H%M%S') + '.log'

        # Create QualityChecker instance to validate data
        if not data_handler.set_quality_checker(validation_error_log,
                                                Params.RULES_DIR):
            sys.exit(1)

        # Move/copy the records from source project to destination project
        logging.info(
            '================= STARTING data transfer ==================')
        data_handler.transfer_data(Params.BATCH_SIZE, Params.MOVE_RECORDS)
        logging.info(
            '================== ENDING data transfer ===================')


if __name__ == '__main__':
    main()
