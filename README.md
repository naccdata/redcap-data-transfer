# data-validator
Module for validating submitted form data in a source REDCap project and copy/move them to a destination REDCap project

Usage: python3 data_validator.py <configuration file path>

Configuration file should be in following JSON format

{
    "src_api_token" : "<API token for sorce REDCap project>",
    "dest_api_token": "<API token for destination REDCap project>",
    "src_api_url": "<URL for source REDCap instance>",
    "dest_api_url": "<URL for destination REDCap instance> (can be same as source URL)",
    "batch_size": <number of records to be processed at a time>,
    "move_records": <0 - copy records, 1 - move records (records will be deleted from source project)>
}
