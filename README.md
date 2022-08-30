# redcap-data-transfer

This module retrieves form data from a source REDCap project, validates the records arrocrding to data quality rules, and writes the records which pass the validation to a destination REDCap project.

**Usage:** python3 redcap_transfer.py

**Set the following environment variables before running the program,**

**Required:**
- SRC_API_TOKEN: "API token for sorce REDCap project"
- DEST_API_TOKEN: "API token for destination REDCap project"
- SRC_API_URL: "URL for source REDCap instance"
- DEST_API_URL: "URL for destination REDCap instance" - (can be same as the source URL)
- RULES_DIR: "Location of data quality rule definition files"

**Optional:**
- BATCH_SIZE: "Number of records to be processed at a time" - (default=100)
- MOVE_RECORDS: "0 - copy records, 1 - move records [records will be deleted from source project]" -  (default=1)
- LOG_FILE_PREFIX: "Prefix for the log file name" - (default="redcap-transfer-")
- LOG_FILE_DIR: "Location to save the log file" - (default="./logs/")
- RULE_DEFS_TYPE: "Rule definitions file type - yaml or json" - (default="yaml")
- STRICT_MODE: "0 - relaxed rule evaluation, 1 - strict rule evaluation" - (default=1)

**Notes**
- Set BATCH_SIZE to -1 to process all records in one batch - not recommended for projects with large number of records
- If STRICT_MODE=1, rule definitions are required for all the forms and all the variables, set this to 0 for testing with a subset
