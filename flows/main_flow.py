from prefect import flow, task, get_run_logger
from prefect.runners import ConcurrentTaskRunner
import subprocess
import requests
import os
from prefect_aws.s3 import s3_upload
from prefect_aws import AwsCredentials
from prefect_meemoo.triplydb.credentials import TriplyDBCredentials
from prefect_meemoo.config.last_run import get_last_run_config, save_last_run_config


# Task to execute SPARQL query via API call and get file list
@task()
def get_url_list(
    triplydb_credentials: TriplyDBCredentials,
    sparql_endpoint: str,
    since: str = None,
):
    logger = get_run_logger()

    filter_clause = ""
    if since is not None:
        filter_clause = f"""
        ?id prov:wasDerivedFrom/schema:dateModified ?modified .
        FILTER (?modified >= {since} )
        """

    sparql_query = f"""
    PREFIX premis: <http://www.loc.gov/premis/rdf/v3/>
    PREFIX schema: <https://schema.org/>
    PREFIX haObj: <https://data.hetarchief.be/ns/object/> 
    PREFIX rel: <http://id.loc.gov/vocabulary/preservation/relationshipSubType/> 
    PREFIX ebucore: <http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#> 
    prefix prov: <http://www.w3.org/ns/prov#>
    prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT DISTINCT ?alto_url
    WHERE {{
        ?id a premis:File;
            premis:storedAt/rdf:value ?alto_url ;
            ebucore:hasMimeType "application/xml" ;
            rel:isi/haObj:isTranscriptionCopyOf ?ie.
              
        {filter_clause}
    }}
    """

    logger.info(f"Executing query on {sparql_endpoint}: {sparql_query}")

    response = requests.get(
        sparql_endpoint,
        params={"query": sparql_query},
        headers={
            "Authorization": "Bearer " + triplydb_credentials.token.get_secret_value(),
            "Accept": "application/sparql-results+json",
        },
    )
    response.raise_for_status()
    results = response.json()
    url_list = [binding["alto_url"]["value"] for binding in results["results"]["bindings"]]
    return url_list


# Task to run the Node.js script and capture stdout as JSON
@task()
def run_node_script(url):
    logger = get_run_logger()
    try:
        # Run the Node.js script using subprocess
        result = subprocess.run(
            ["node", "../script/extract-text-lines-from-alto.js", url],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise Exception(f"Error running script for {url}: {result.stderr}")
        return url, result.stdout
    except Exception as e:
        logger.error(f"Failed to process {url}: {str(e)}")
        raise e


# Task to upload the JSON output to S3
@task()
def upload_to_s3(
    content: tuple[str, str], s3_bucket_name: str, s3_credentials: AwsCredentials
):

    logger = get_run_logger()

    file_name = os.path.basename(content[0])
    s3_key = f"{file_name}.json"
    s3_upload(
        bucket=s3_bucket_name,
        key=s3_key,
        data=content[1],
        aws_credentials=s3_credentials,
    )
    logger.info(f"Uploaded {file_name} to s3://{s3_bucket_name}/{s3_key}")


@flow(
    name="prefect_flow_arc_alto_to_json",
    task_runner=ConcurrentTaskRunner(),
    on_completion=[save_last_run_config],
)
def main_flow(
    s3_bucket_name: str = "bucket",
    s3_block_name: str = "object-store",
    triplydb_block_name: str = "triplydb",
    sparql_endpoint: str = "https://api.meemoo-int.triply.cc/datasets/meemoo/knowledge-graph/services/knowledge-graph/sparql",
    full_sync: bool = False,
):
    # Load credentials
    triply_creds = TriplyDBCredentials.load(triplydb_block_name)
    s3_credentials = AwsCredentials.load(s3_block_name)

    # Figure out start time
    if not full_sync:
        last_modified_date = get_last_run_config("%Y-%m-%d")

    url_list = get_url_list(
        triply_creds,
        sparql_endpoint,
        since=last_modified_date if not full_sync else None,
    )
    outputs = run_node_script.map(url_list)
    upload_results = upload_to_s3.map(outputs, s3_credentials, s3_bucket_name)
    return upload_results


if __name__ == "__main__":
    main_flow()
