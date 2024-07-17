from prefect import flow, task, get_run_logger
from datetime import datetime
from prefect.runners import ConcurrentTaskRunner
import subprocess
import requests
import os
from prefect_aws.s3 import s3_upload
from prefect_aws import AwsCredentials
from prefect_meemoo.triplydb.credentials import TriplyDBCredentials
from prefect_meemoo.config.last_run import get_last_run_config, save_last_run_config

# SPARQL endpoint and query
SPARQL_ENDPOINT = "http://example.com/sparql"
SPARQL_QUERY = """
SELECT ?file WHERE {
  ?s ?p ?file .
  FILTER (regex(str(?file), ".*\\.txt$"))
}
"""

# Task to execute SPARQL query via API call and get file list
@task
def get_url_list(triplydb_credentials: TriplyDBCredentials, since: datetime=None):
    response = requests.get(
        SPARQL_ENDPOINT.format(since), params={"query": SPARQL_QUERY}, 
        headers={
                "Authorization": "Bearer " + triplydb_credentials.token.get_secret_value(),
                "Accept": "application/sparql-results+json",
            },
    )
    response.raise_for_status()
    results = response.json()
    url_list = [binding["file"]["value"] for binding in results["results"]["bindings"]]
    return url_list


# Task to run the Node.js script and capture stdout as JSON
@task
def run_node_script(url):
    logger = get_run_logger()
    try:
        # Run the Node.js script using subprocess
        result = subprocess.run(
            ["node", "processFile.js", url], capture_output=True, text=True
        )
        if result.returncode != 0:
            raise Exception(f"Error running script for {url}: {result.stderr}")
        return url, result.stdout
    except Exception as e:
        logger.error(f"Failed to process {url}: {str(e)}")
        raise e


# Task to upload the JSON output to S3
@task
def upload_to_s3(
    content: tuple[str, str], 
    s3_bucket_name: str, 
    s3_credentials: AwsCredentials):

    file_name = os.path.basename(content[0])
    s3_key = f"{file_name}.json"
    s3_upload(
        bucket=s3_bucket_name,
        key=s3_key,
        data=content[1],
        aws_credentials=s3_credentials,
    )
    print(f"Uploaded {file_name} to s3://{s3_bucket_name}/{s3_key}")


@flow(name="prefect_flow_arc_alto_to_json", task_runner=ConcurrentTaskRunner(),  on_completion=[save_last_run_config])
def main_flow(
    s3_bucket_name: str = "your-bucket-name",
    s3_block_name: str = "",
    triplydb_block_name: str = "triplydb",
    full_sync: bool = False
):
    # Load credentials
    triply_creds = TriplyDBCredentials.load(triplydb_block_name)
    s3_credentials = AwsCredentials.load(s3_block_name)

    # Figure out start time
    if not full_sync:
        last_modified_date = get_last_run_config("%Y-%m-%d")

    url_list = get_url_list(triply_creds, since=last_modified_date if not full_sync else None)
    outputs = run_node_script.map(url_list)
    upload_results = upload_to_s3.map(outputs, s3_credentials, s3_bucket_name)

if __name__ == "__main__":
    main_flow()