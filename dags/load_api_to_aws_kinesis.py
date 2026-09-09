from __future__ import annotations

import json
import logging
from datetime import datetime

import boto3
import requests
from airflow import DAG
from airflow.models import Variable

# On Airflow 3 the canonical path is:
#   from airflow.providers.standard.operators.python import PythonOperator
# The line below still resolves via the compat shim; switch when you're ready.
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

API_BASE_URL = "https://jsonplaceholder.typicode.com"
STREAM_NAME = "dea-airflow-user-post-data"
MAX_USER_ID = 10


def _set_api_user_id(**context):
    """Advance the incremental userId cursor and return the new value."""
    try:
        current = int(Variable.get("api_user_id", default_var=-1))
        logger.info("current api_user_id:: %s", current)

        next_id = 1 if current in (-1, MAX_USER_ID) else current + 1
        Variable.set(key="api_user_id", value=next_id)

        logger.info("api_user_id advanced to %s", next_id)
        return next_id
    except Exception:
        logger.exception("ERROR WHILE SETTING UP userId param value")
        raise


def _extract_userposts(new_api_user_id, **context):
    """Fetch posts for a single userId from the API."""
    try:
        user_id = int(new_api_user_id)
        response = requests.get(
            f"{API_BASE_URL}/posts",
            params={"userId": user_id},
            timeout=30,
        )
        response.raise_for_status()
        user_posts = response.json()

        logger.info("fetched %s posts for userId=%s", len(user_posts), user_id)
        return user_posts
    except Exception:
        logger.exception("ERROR WHILE FETCHING USER POSTS API DATA")
        raise


def _process_user_posts(new_api_user_id, **context):
    """Write each post to the Kinesis stream, preserving per-shard ordering."""
    try:
        user_posts = context["ti"].xcom_pull(task_ids="extract_userposts")
        if not user_posts:
            logger.info("no posts to write for userId=%s", new_api_user_id)
            return f"No posts found for user id {new_api_user_id}"

        # Client built at task runtime, not DAG parse time.
        kinesis_client = boto3.client("kinesis")

        last_sequence_number = None
        for user_post in user_posts:
            put_kwargs = {
                "StreamName": STREAM_NAME,
                "Data": json.dumps(user_post).encode("utf-8") + b"\n",
                "PartitionKey": str(user_post["userId"]),
            }
            # SequenceNumberForOrdering must be a sequence number returned by a
            # previous PutRecord on the same partition key -- so chain, don't guess.
            if last_sequence_number:
                put_kwargs["SequenceNumberForOrdering"] = last_sequence_number

            response = kinesis_client.put_record(**put_kwargs)
            last_sequence_number = response["SequenceNumber"]

            logger.info(
                "wrote record %s to shard %s (status %s, retries %s)",
                response["SequenceNumber"],
                response["ShardId"],
                response["ResponseMetadata"]["HTTPStatusCode"],
                response["ResponseMetadata"]["RetryAttempts"],
            )

        return (
            f"Total {len(user_posts)} posts with user id {new_api_user_id} "
            f"written to kinesis stream `{STREAM_NAME}`"
        )
    except Exception:
        logger.exception("ERROR WHILE WRITING USER POSTS TO KINESIS STREAM")
        raise


with DAG(
    dag_id="load_api_aws_kinesis",
    default_args={"owner": "Sovan"},
    tags=["api", "kinesis"],
    start_date=datetime(2023, 9, 24),
    schedule="@daily",
    catchup=False,
):
    # Rendered at execution time, after get_api_userId_params has run.
    user_id_template = "{{ ti.xcom_pull(task_ids='get_api_userId_params') }}"

    get_api_userId_params = PythonOperator(
        task_id="get_api_userId_params",
        python_callable=_set_api_user_id,
    )

    extract_userposts = PythonOperator(
        task_id="extract_userposts",
        python_callable=_extract_userposts,
        op_kwargs={"new_api_user_id": user_id_template},
    )

    write_userposts_to_stream = PythonOperator(
        task_id="write_userposts_to_stream",
        python_callable=_process_user_posts,
        op_kwargs={"new_api_user_id": user_id_template},
    )

    get_api_userId_params >> extract_userposts >> write_userposts_to_stream