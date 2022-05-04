# redcap-data-transfer
Module for copying/moving submitted form data in a source REDCap project to a destination REDCap project

Usage: python3 data_validator.py [\<configurations file path\>]

Set the following environment variables before running the program
<br><br>
Required variables
<ul>
  <li>   SRC_API_TOKEN: "API token for sorce REDCap project" </li>
  <li>   DEST_API_TOKEN: "API token for destination REDCap project" </li>
  <li>   SRC_API_URL: "URL for source REDCap instance" </li>
  <li>   DEST_API_URL: "URL for destination REDCap instance" - (can be same as the source URL) </li>
</ul>
<br>

Optional variables
<br>
<ul>
  <li>   BATCH_SIZE: "Number of records to be processed at a time" - (default=100, set to -1 to process all records in one batch) </li>
  <li>   MOVE_RECORDS: "0 - copy records, 1 - move records [records will be deleted from source project]" -  (default=0) </li>
</ul>
<br>

Optional configurations JSON file can be given to filter the forms and events

{ <br>
  &nbsp; &nbsp;   "forms": "\<comma separated list of desired forms\>" <br>
  &nbsp; &nbsp;   "events": "\<comma separated list of desired events\>" <br>
}
