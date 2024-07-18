from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
import subprocess
import os
import psycopg2
import json
from prefect_sqlalchemy.credentials import DatabaseCredentials
from prefect_aws.s3 import s3_upload
from prefect_aws import AwsCredentials
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
@task()
def run_node_script(content: tuple[str, str]):
    logger = get_run_logger()
    representation_id, url = content
    
    try:
        # Run the Node.js script using subprocess
        result = subprocess.run(
            ["node", "script/extract-text-lines-from-alto.js", url],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise Exception(f"Error running script for {url}: {result.stderr}")
        return representation_id, url, result.stdout
    except Exception as e:
        logger.error(f"Failed to process {url}: {str(e)}")
        raise e


# Task to upload the JSON output to S3
@task()
def upload_to_s3(
    content: tuple[str, str, str], 
    s3_credentials: AwsCredentials,
    s3_bucket_name: str
):
    logger = get_run_logger()
    representation_id, url, json_string = content

    file_name = os.path.basename(url)
    s3_key = f"{file_name}.json"
    key = s3_upload(
        bucket=s3_bucket_name,
        key=s3_key,
        data=json_string,
        aws_credentials=s3_credentials,
    )
    s3_url = f"s3://{s3_bucket_name}/{s3_key}"
    logger.info(f"Uploaded {file_name} to {s3_url}")
    return representation_id, s3_url, json_string


@task()
def insert_schema_transcript(
    content: tuple[str, str],
    postgres_credentials: DatabaseCredentials,
):
    logger = get_run_logger()

    representation_id, url, json_string = content

    # process JSON
    parsed_json = json.loads(json_string)
    concatenated_text = "".join(item["text"] for item in parsed_json["text"])

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
        (concatenated_text, representation_id),
    )
    # insert url into table
    logger.info("Inserting schema_transcript_url into 'graph.schema_transcript_url'")
    cur.execute(
        "INSERT INTO graph.schema_transcript_url (representation_id, schema_transcript_url) VALUES (%s, %s)",
        (representation_id, url),
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
    s3_bucket_name: str = "bucket",
    s3_block_name: str = "object-store",
    postgres_block_name: str = "postgres",
    full_sync: bool = False,
):
    # Load credentials
    postgres_creds = DatabaseCredentials.load(postgres_block_name)
    s3_creds = AwsCredentials.load(s3_block_name)

    # Figure out start time
    if not full_sync:
        last_modified_date = get_last_run_config("%Y-%m-%d")

    url_list = get_url_list(
        postgres_creds,
        since=last_modified_date if not full_sync else None,
    )
    outputs = run_node_script.map(url_list)
    upload_results = upload_to_s3.map(outputs, s3_credentials=s3_creds, s3_bucket_name=s3_bucket_name)
    insert_transcripts = insert_schema_transcript.map(upload_results, postgres_credentials=postgres_creds)
    return insert_transcripts


if __name__ == "__main__":
    main_flow()
