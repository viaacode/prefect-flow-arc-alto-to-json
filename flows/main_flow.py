from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.futures import PrefectFuture
import subprocess
import os
import psycopg2
import json
from prefect_sqlalchemy.credentials import DatabaseCredentials
from prefect_aws.s3 import s3_upload
from prefect_aws import AwsCredentials, AwsClientParameters
from prefect_meemoo.config.last_run import get_last_run_config, save_last_run_config


# Task to execute SPARQL query via API call and get file list
@task()
def get_url_list(
    postgres_credentials: DatabaseCredentials,
    since: str = None,
):
    logger = get_run_logger()

    sql_query = """
    SELECT representation_id, premis_stored_at
    FROM graph.file f
    JOIN graph.includes i ON i.file_id = f.id
    WHERE f.ebucore_has_mime_type = 'application/xml' AND schema_name LIKE '%alto%'
    """

    if since is not None:
        sql_query += f" AND f.updated_at >= {since}"

    # Step 1: Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        user=postgres_credentials.username,
        password=postgres_credentials.password.get_secret_value(),
        host=postgres_credentials.host,
        port=postgres_credentials.port,
        database=postgres_credentials.database,
        # connection_factory=LoggingConnection,
    )
    logger.info(f"Executing query on {postgres_credentials.host}: {sql_query}")
    cur = conn.cursor()
    cur.execute(sql_query)
    return cur.fetchall()


# Task to run the Node.js script and capture stdout as JSON
@task(task_run_name="create-json-from-{url}")
def run_node_script(url: str):
    logger = get_run_logger()

    try:
        # Run the Node.js script using subprocess
        result = subprocess.run(
            ["node", "script/extract-text-lines-from-alto.js", url],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise Exception(f"Error running script for {url}: {result.stderr}")
        return result.stdout
    except Exception as e:
        logger.error(f"Failed to process {url}: {str(e)}")
        raise e
@task()
def extract_transcript(json_string: str):
    # process JSON
    parsed_json = json.loads(json_string)
    # get the full text
    return " ".join(item["text"] for item in parsed_json["text"])


@task(task_run_name="insert-{s3_url}-in-database")
def insert_schema_transcript(
    representation_id: str,
    s3_url: str,
    transcript: str,
    postgres_credentials: DatabaseCredentials,
):
    logger = get_run_logger()

    # connect to database
    conn = psycopg2.connect(
        user=postgres_credentials.username,
        password=postgres_credentials.password.get_secret_value(),
        host=postgres_credentials.host,
        port=postgres_credentials.port,
        database=postgres_credentials.database,
        # connection_factory=LoggingConnection,
    )
    cur = conn.cursor()
    logger.info("Inserting transcript into 'graph.representation'")
    # insert transcript into table
    cur.execute(
        "INSERT INTO graph.representation (schema_transcript) VALUES (%s) WHERE representation_id = %s",
        (transcript, representation_id),
    )
    # insert url into table
    logger.info("Inserting schema_transcript_url into 'graph.schema_transcript_url'")
    cur.execute(
        "INSERT INTO graph.schema_transcript_url (representation_id, schema_transcript_url) VALUES (%s, %s)",
        (representation_id, s3_url),
    )
    conn.commit()

    # Step 5: Clean up and close the connection
    cur.close()
    conn.close()


@flow(
    name="prefect_flow_arc_alto_to_json",
    task_runner=ConcurrentTaskRunner(),
    on_completion=[save_last_run_config],
)
def main_flow(
    s3_endpoint: str = "http://assets-int.hetarchief.be", 
    s3_bucket_name: str = "hetarchief-int",
    s3_block_name: str = "arc-object-store",
    postgres_block_name: str = "postgres",
    full_sync: bool = False,
):
    # Load credentials
    postgres_creds = DatabaseCredentials.load(postgres_block_name)
    s3_creds = AwsCredentials.load(s3_block_name)
    client_parameters = AwsClientParameters(endpoint_url=s3_endpoint)

    # Figure out start time
    if not full_sync:
        last_modified_date = get_last_run_config("%Y-%m-%d")

    url_list = get_url_list.submit(
        postgres_creds,
        since=last_modified_date if not full_sync else None,
    ).result()
    for representation_id, url in url_list:
        json_string = run_node_script.submit(url=url)
        transcript = extract_transcript.submit(
            json_string=json_string.result()
        )
        s3_key = s3_upload.submit(
            bucket=s3_bucket_name,
            key=f"{os.path.basename(url)}.json",
            data=json_string.result().encode(),
            aws_credentials=s3_creds,
            aws_client_parameters=client_parameters
        )
        result = insert_schema_transcript.submit(
            representation_id=representation_id,
            s3_url=f"{s3_endpoint}/{s3_bucket_name}/{s3_key.result()}",
            transcript=transcript.result(),
            postgres_credentials=postgres_creds
        )


if __name__ == "__main__":
    main_flow()
