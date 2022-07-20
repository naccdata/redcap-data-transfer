#!/usr/bin/env python3

import logging
import sys

from datetime import datetime as dt

from data_handler import DataHandler
from params import Params
from redcap_connection import REDCapConnection, REDCapConnectionException


def main():
    """ Programm entry """
    start_time = dt.now()

    # Set up logger
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] - %(message)s',
                        datefmt='%m-%d-%y %H:%M:%S')

    # Load and validate the project parameters
    if not Params.load_parameters():
        sys.exit(1)

    setup_logfile()

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
    if 'forms' in Params.EXTRA_PARAMS and \
        Params.EXTRA_PARAMS['forms'].strip():
        forms = [x.strip() for x in Params.EXTRA_PARAMS['forms'].split(',')]

    events = None
    if 'events' in Params.EXTRA_PARAMS and \
        Params.EXTRA_PARAMS['events'].strip():
        events = [x.strip() for x in Params.EXTRA_PARAMS['events'].split(',')]

    qc_err_form = 'data_quality_check_errors'
    if 'errors_form' in Params.EXTRA_PARAMS:
        qc_err_form = Params.EXTRA_PARAMS['errors_form']

    data_handler = DataHandler(src_project, dest_project, forms, events,
                               qc_err_form)

    # Check whether source and destination project settings matches,
    logging.info('Comparing source and destination project compatibility...')
    if data_handler.compare_project_settings():
        # Create QualityChecker instance to validate data
        if not data_handler.set_quality_checker(Params.RULES_DIR,
                                                Params.STRICT_MODE):
            sys.exit(1)

        # Move/copy the records from source project to destination project
        logging.info(
            '================= STARTING data transfer ==================')
        data_handler.transfer_data(Params.BATCH_SIZE, Params.MOVE_RECORDS)
        logging.info(
            '================== ENDING data transfer ===================')

    end_time = dt.now()
    logging.info('Total runtime - %s', end_time - start_time)


def setup_logfile():
    """ Add a file handler to the root logger """

    current_time = dt.now()
    log_file = Params.LOG_FILE_DIR + Params.LOG_FILE_PREFIX
    log_file += current_time.strftime('%m%d%y-%H%M%S') + '.log'

    logger = logging.getLogger()

    # Create file handler
    try:
        f_handler = logging.FileHandler(log_file)

        # Create formatter and add it to handler
        f_format = logging.Formatter(
            fmt='%(asctime)s [%(levelname)s] - %(message)s',
            datefmt='%m-%d-%y %H:%M:%S')
        f_handler.setFormatter(f_format)

        f_handler.setLevel(logging.INFO)

        # Add handler to the logger
        logger.addHandler(f_handler)

        logging.info('Application log file created - %s', log_file)
    except FileNotFoundError as e:
        logging.error('Failed to set up application log file - %s : %s',
                      log_file, e.strerror)


if __name__ == '__main__':
    main()
