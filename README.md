# redcap-data-transfer
Module for copying/moving submitted form data in a source REDCap project to a destination REDCap project

Usage: python3 data_validator.py \<configuration file path\>

Configuration file should be in the following JSON format

{ <br>
  &nbsp; &nbsp;   "src_api_token" : "[API token for sorce REDCap project]", <br>
  &nbsp; &nbsp;   "dest_api_token": "[API token for destination REDCap project]", <br>
  &nbsp; &nbsp;   "src_api_url": "[URL for source REDCap instance]", <br>
  &nbsp; &nbsp;   "dest_api_url": "[URL for destination REDCap instance] (can be same as the source URL)", <br>
  &nbsp; &nbsp;   "batch_size": [number of records to be processed at a time, -1 to process all in one batch], <br>
  &nbsp; &nbsp;   "move_records": [0 - copy records, 1 - move records (records will be deleted from source project)] <br>
  &nbsp; &nbsp;   "forms": "[comma separated list of desired forms, optional]" <br>
  &nbsp; &nbsp;   "events": "[comma separated list of desired events, optional]" <br>
}
