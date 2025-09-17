import os

import psycopg2
import psycopg2.extras
from pendulum.datetime import DateTime
from prefect import flow, get_run_logger, task
from prefect.states import Failed, Completed
from prefect.task_runners import ConcurrentTaskRunner
from prefect_aws import AwsCredentials
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
    SELECT i.representation_id, f.premis_stored_at, t.updated_at
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
    logger.info(f"Executing query on {postgres_credentials.host}: {sql_query}")
    cur = conn.cursor()

    if since is not None:
        sql_query += " AND f.updated_at >= %(since)s"
        cur.execute(sql_query, {"since": since})
    else:
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
    skip_unmodified: bool = True,
    replace_url: tuple[str, str] = ("", ""),
) -> list[str, str, str]:
    logger = get_run_logger()

    s3_endpoint = (
        s3_base_url
        if s3_base_url is not None
        else s3_credentials.aws_client_parameters.endpoint_url
    )

    count = 0
    skipped = 0
    empty = 0
    output = []
    logger.info("Processing batch of %s representations.", len(batch))
    for representation_id, url, updated_at in batch:
        s3_key = f"{os.path.basename(url)}.json"
        try:
            # WORKAROUND for secure URLs: replace domain of AltoXML URL
            if replace_url[0] is not None and replace_url[1] is not None:
                url = url.replace(replace_url[0], replace_url[1])

            # Optionally skip files that haven't been modified
            if (not skip_unmodified) or is_alto_modified(url, updated_at):
                # Get the JSON 
                transcript: SimplifiedAlto = convert_alto_xml_url_to_simplified_json(
                    url
                )
                transcript_text = transcript.to_transcript()
                # Only process non empty transcripts
                if transcript_text:
                    # Get S3 client
                    s3_client = s3_credentials.get_boto3_session().client(
                        "s3",
                        config=Config(
                            request_checksum_calculation="when_required",
                            response_checksum_validation="when_required",
                        ),
                        **s3_credentials.aws_client_parameters.get_params_override(),
                    )

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
                            f"{s3_endpoint}/{s3_bucket_name}/{s3_key}?domain={s3_domain}",
                            transcript_text,
                        ),
                    )
                else:
                    empty +=1
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
        if (succeeded + skipped) < total:
            failed = total - succeeded
            return Failed(
                message=f"Batch failed: {failed}/{total} items not processed ({skipped} skipped unmodified)."
            )
        return Completed(
            message=f"Batch succeeded: {succeeded}/{total} items processed ({skipped} skipped unmodified)."
        )
    except Exception as e:
        logger.exception("Failed to insert batch.")
        raise e


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
            skip_unmodified=skip_unmodified,
            replace_url=replace_url,
        )
