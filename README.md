# prefect-flow-arc-alto-to-json

This prefect flow 
- transforms each AltoXML into a simplified JSON format and uploads the result to a S3 bucket,
- stores the S3 link and the text content of each AltoXML file in a postgres table 

This implementation consist of 
- `flows/main_flow.py`: a Prefect Flow (Python) that orchestrates the necessary steps and 
- `flows/convert_alto_to_simplified_json.py`: a couple of helper functions that a.o. tranfrom XML to the JSON output.

The latter is run by the former. Here's a quick overview on both components

### 1. flows/main_flow.py
- the flow retrieves all AltoXML URLs from the hetarchief.be application postgres database
- the URLs are processed in batches:
  - each AltoXML is downloaded from S3 and parsed
  - the XML contents are transformed to JSON using the `convert_alto_xml_url_to_simplified_json()` function from `flows/convert_alto_to_simplified_json.py`
  - the result is uploaded to an S3 bucket, which results in a S3 URL
  - all S3 URLs and the transcript (= the file contents stripped from JSON syntax) are upserted in a table in the hetarchief.be application postgres database

### 2. flows/convert_alto_to_simplified_json.py

- there are a couple of helper functions:
  - `convert_alto_xml_url_to_simplified_json()` converts AltoXML text into a JSON format. It uses a couple of heuristics to determine the AltoXML version and fix a couple of errors.
  - `rewrite_url()` removes the third part of the AltoXML URL. This is a a workaround when the AltoXML is moved.
  - `is_alto_modified()` checks if an URLs target has been modified since a given datetime. This is used to skip unmodified AltoXML files.

## How to run?

### Running the AltoXML to JSON transform

You can transform a single AltoXML URL by running `python flows/convert_alto_to_simplified_json.py <S3 link to AltoXML> > output.alto.json` 

### Running the Prefect Flow

The Prefect Flow requires setting the following parameters:
- `s3_base_url`: S3 base URL for storing the JSON output
- `s3_domain`: the S3 domain (used for access)
- `s3_bucket_name`: the name of the S3 bucket to store the JSON output
- `s3_block_name`: the prefect block storing the S3 credentials
- `db_block_name`: the prefect block storing database credentials
- `batch_size`: the size of the batches
- `last_modified`: date from which modified or processed AltoXML URLs have to be processed
- `full_sync`: process all AltoXML URLs or only process AltoXML that have been added or modified since the `last_modified` date
- `skip_unmodified`: when true, an AltoXML URL is only processed if it has been modified since last run
- `replace_url`: replace a part of the AltoXML URL with another part