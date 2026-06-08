import os

import psycopg2
import psycopg2.extras
from pendulum.datetime import DateTime
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_table_artifact
from prefect.states import Failed, Completed
from prefect.task_runners import ConcurrentTaskRunner
from prefect_aws import AwsCredentials
from botocore.exceptions import ClientError
from prefect_meemoo.config.last_run import save_last_run_config
from prefect_sqlalchemy.credentials import DatabaseCredentials
from botocore.config import Config

from flows.convert_alto_to_simplified_json import (
    SimplifiedAlto,
    convert_alto_xml_url_to_simplified_json,
    is_alto_modified,
)


# Task to execute SPARQL query via API call and get file list
@task
def get_url_list(
    postgres_credentials: DatabaseCredentials,
    since: DateTime = None,
) -> list[tuple[str, str]]:
    logger = get_run_logger()

    sql_query = """
    SELECT i.representation_id, f.premis_stored_at
    FROM graph.file f
    JOIN graph.includes i ON i.file_id = f.id
    LEFT JOIN graph.schema_transcript_url t ON i.representation_id = t.representation_id
    WHERE f.ebucore_has_mime_type IN ('application/xml', 'text/plain') 
    AND schema_name LIKE '%%alto%%'
    """

    # Step 1: Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        user=postgres_credentials.username,
        password=postgres_credentials.password.get_secret_value(),
        host=postgres_credentials.host,
        port=postgres_credentials.port,
        database=postgres_credentials.database,
    )
    cur = conn.cursor()

    if since is not None:
        sql_query += " AND f.updated_at >= %(since)s"
        cur.execute(sql_query, {"since": since})
        logger.info(f"Executing query on {postgres_credentials.host}: {sql_query}, since {since}")
    else:
        logger.info(f"Executing query on {postgres_credentials.host}: {sql_query}")
        cur.execute(sql_query)
    url_list = cur.fetchall()
    logger.info(f"Retrieved {len(url_list)} URLs.")
    return url_list


@task(tags=["etl-alto"])
def create_and_upload_transcript_batch(
    batch: list[str, str],
    postgres_credentials: DatabaseCredentials,
    s3_bucket_name: str,
    s3_credentials: AwsCredentials,
    s3_base_url: str = None,
    s3_domain: str = None,
    last_modified: DateTime = None,
    skip_unmodified: bool = True,
    replace_url: tuple[str, str] = ("", ""),
    fail_tasks: bool = True,
) -> list[str, str, str]:
    logger = get_run_logger()

    s3_endpoint = (
        s3_base_url
        if s3_base_url is not None
        else s3_credentials.aws_client_parameters.endpoint_url
    )

    def get_s3_client():
        return s3_credentials.get_boto3_session().client(
            "s3",
            config=Config(
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
                retries={"mode": "standard", "max_attempts": 3},
            ),
            **s3_credentials.aws_client_parameters.get_params_override(),
        )

    s3_client = get_s3_client()

    def s3_file_exists(bucket_name: str, key: str) -> bool:
        try:
            s3_client.head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    count = 0
    skipped = 0
    empty = 0
    output = []
    logger.info("Processing batch of %s representations.", len(batch))
    failed_representations = []
    for representation_id, url in batch:
        s3_key = f"{os.path.basename(url)}.json"
        s3_file_url = f"{s3_endpoint}/{s3_bucket_name}/{s3_key}?domain={s3_domain}"
        try:
            # WORKAROUND for secure URLs: replace domain of AltoXML URL
            if replace_url[0] is not None and replace_url[1] is not None:
                url = url.replace(replace_url[0], replace_url[1])
            
            # Optionally skip files that haven't been modified
            if (not skip_unmodified) or not s3_file_exists(url, s3_key) or is_alto_modified(s3_file_url, since=last_modified):
                # Get the JSON 
                transcript: SimplifiedAlto = convert_alto_xml_url_to_simplified_json(
                    url
                )
                transcript_text = transcript.to_transcript()
                # Only process non empty transcripts
                if transcript_text:
                    # Upload JSON file to S3
                    s3_client.put_object(
                        Bucket=s3_bucket_name,
                        Key=s3_key,
                        Body=str(transcript).encode("utf-8"),
                    )

                    # Append the JSON S3 URL and transcript to what needs to be stored in the database
                    output.append(
                        (
                            representation_id,
                            s3_file_url,
                            transcript_text,
                        ),
                    )
                else:
                    empty +=1
                    failed_representations.append({ "representation": url, "s3_key": s3_key, "type": "empty" })
                    logger.warning("Empty transcript for %s of representation %s skipped.", url, representation_id)
            else:
                skipped += 1

            # Print progress in 10 updates
            count += 1
            if count % (len(batch) / 10) == 0:
                logger.info(
                    "S3 Upload %s%% done. Last representation %s had key %s to bucket %s (skipped unmodified: %s; empty: %s).",
                    round((len(output) / len(batch)) * 100),
                    representation_id,
                    s3_key,
                    s3_bucket_name,
                    skipped,
                    empty
                )

        except Exception:
            failed_representations.append({ "representation": url, "s3_key": s3_key, "type": "failed" })
            logger.exception(
                "Failed to process Alto XML at %s to endpoint %s and bucket %s with key %s.",
                url,
                s3_endpoint,
                s3_bucket_name,
                s3_key,
            )

    try:
        # Upsert the batch into database table
        insert_schema_transcript_batch(
            output, postgres_credentials=postgres_credentials
        )

        total = len(batch)
        succeeded = len(output)
        if failed_representations:
            failed = total - succeeded - skipped - empty
            create_table_artifact(
                key="failed-alto-representations",
                table=failed_representations,
                description="List of representations that failed to be processed in this batch.",
            )
            logger.error(
                f"Batch failed: {failed}/{total} items not processed due to error ({skipped} skipped unmodified; {empty} empty transcripts)."
            )
            if fail_tasks:
                return Failed(
                    message=f"Batch failed: {failed}/{total} items not processed due to error ({skipped} skipped unmodified; {empty} empty transcripts)."
                )
            else:
                return Completed(
                    message=f"Batch failed: {failed}/{total} items not processed due to error ({skipped} skipped unmodified; {empty} empty transcripts)."
                )
        return Completed(
            message=f"Batch succeeded: {succeeded}/{total} items processed ({skipped} skipped unmodified; {empty} empty transcripts)."
        )
    except Exception as e:
        logger.exception("Failed to insert batch.")
        raise e
    finally:
        s3_client.close()


# @task
def insert_schema_transcript_batch(
    batch: list[str, str, str],
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
    )
    cur = conn.cursor()

    # insert url into table
    logger.info("Inserting %s URLs into 'graph.schema_transcript_url'.", len(batch))
    insert_query = """
        INSERT INTO graph.schema_transcript_url (representation_id, schema_transcript_url, schema_transcript) 
        VALUES %s 
        ON CONFLICT(representation_id) 
        DO UPDATE SET schema_transcript_url = EXCLUDED.schema_transcript_url, schema_transcript = EXCLUDED.schema_transcript;
        """
    psycopg2.extras.execute_values(
        cur,
        insert_query,
        batch,
        template=None,
        page_size=100,
    )
    conn.commit()
    logger.info("URLs inserted into 'graph.schema_transcript_url'.")

    # Step 5: Clean up and close the connection
    cur.close()
    conn.close()


@flow(
    name="prefect_flow_arc_alto_to_json",
    task_runner=ConcurrentTaskRunner(),
    on_completion=[save_last_run_config],
)
def main_flow(
    s3_base_url: str = "http://swarmget.do.viaa.be",
    s3_domain: str = "s3-int.viaa.be",
    s3_bucket_name: str = "hetarchief",
    s3_block_name: str = "arc-object-store",
    db_block_name: str = "local",
    batch_size: int = 100,
    last_modified: DateTime = None,
    full_sync: bool = False,
    skip_unmodified: bool = True,
    replace_url: tuple[str, str] = ("", ""),
    fail_tasks: bool = True,
):
    logger = get_run_logger()

    # Load credentials
    postgres_creds = DatabaseCredentials.load(db_block_name)
    s3_credentials = AwsCredentials.load(s3_block_name)

    logger.info("Last run: %s", last_modified)

    # Get all AltoXML URLs from database
    url_list = get_url_list(
        postgres_creds,
        since=last_modified if not full_sync else None,
    )

    # Process AltoXML URLs in batches
    for i in range(0, len(url_list), batch_size):
        batch = url_list[i : i + batch_size]

        create_and_upload_transcript_batch.submit(
            batch,
            postgres_credentials=postgres_creds,
            s3_bucket_name=s3_bucket_name,
            s3_credentials=s3_credentials,
            s3_base_url=s3_base_url,
            s3_domain=s3_domain,
            last_modified=last_modified if not full_sync else None,
            skip_unmodified=skip_unmodified,
            replace_url=replace_url,
            fail_tasks=fail_tasks,
        )
