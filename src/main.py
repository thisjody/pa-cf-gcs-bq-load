from google.cloud import bigquery, secretmanager, pubsub_v1
from google.oauth2 import service_account
from google.auth import impersonated_credentials
from google.api_core.exceptions import NotFound, Forbidden
import logging
import base64
import json
import os
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)
PROJECT_ID = os.getenv('PROJECT_ID')

IMPERSONATE_SA_MAP = {
    'publish': os.environ['PUBLISH_SA'],
    'load': os.environ['LOAD_SA']
}

def get_secret(secret_name):
    """Retrieve secrets from Google Secret Manager for permission elevation."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    return json.loads(response.payload.data.decode('UTF-8'))

def get_impersonated_credentials(action='load'):
    """Retrieve impersonated credentials."""
    sa_credentials_secret_name = os.getenv('SA_CREDENTIALS_SECRET_NAME')
    sa_credentials = get_secret(sa_credentials_secret_name)
    credentials = service_account.Credentials.from_service_account_info(sa_credentials)
    target_principal = IMPERSONATE_SA_MAP.get(action)
    if not target_principal:
        raise ValueError(f"No service account mapped for action: {action}")
    target_scopes = os.getenv('TARGET_SCOPES').split(",")
    return impersonated_credentials.Credentials(
        source_credentials=credentials,
        target_principal=target_principal,
        target_scopes=target_scopes,
        lifetime=600
    )

def check_and_create_dataset(bq_client, dataset_name, location='US'):
    """Check if dataset exists, if not create it."""
    dataset_id = f"{PROJECT_ID}.{dataset_name}"
    try:
        dataset = bq_client.get_dataset(dataset_id)  # Make an API request.
        logging.info(f"Dataset {dataset_name} already exists at location {dataset.location}.")
    except NotFound as e:
        # Here is where you add the logging for the exception
        logging.error(f"Exception details: {e}")
        
        logging.info(f"Dataset {dataset_name} not found. Creating it now at location {location}.")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        try:
            created_dataset = bq_client.create_dataset(dataset, timeout=30)  # Make an API request.
            logging.info(f"Created dataset {created_dataset.project}.{created_dataset.dataset_id} at location {created_dataset.location}")
        except Exception as e:
            # If there's an error in creating the dataset, log that exception as well.
            logging.error(f"Failed to create dataset: {e}")
            raise


def check_and_create_table(bq_client, dataset_name, table_name):
    """Check if a table exists within the dataset, if not create it."""
    table_id = f"{PROJECT_ID}.{dataset_name}.{table_name}"
    try:
        table = bq_client.get_table(table_id)  # Make an API request.
        logging.info(f"Table {table_name} already exists in dataset {dataset_name}.")
    except NotFound as e:
        logging.info(f"Table {table_name} not found in dataset {dataset_name}. Creating it now.")
        table = bigquery.Table(table_id)
        try:
            created_table = bq_client.create_table(table, timeout=30)  # Make an API request.
            logging.info(f"Created table {created_table.project}.{created_table.dataset_id}.{created_table.table_id}")
        except Exception as e:
            logging.error(f"Failed to create table: {e}")
            raise


def publish_to_topic(topic_name, data):
    """Publish data to a Pub/Sub topic."""
    try:
        # Get the impersonated credentials for the 'publish' action
        credentials = get_impersonated_credentials(action='publish')

        # Initialize the Pub/Sub publisher client with the impersonated credentials
        publisher = pubsub_v1.PublisherClient(credentials=credentials)

        # Construct the topic path
        topic_path = publisher.topic_path(PROJECT_ID, topic_name)

        # Convert data to JSON string and encode it to bytes
        message_json = json.dumps(data)
        message_bytes = message_json.encode('utf-8')

        # Publish the message
        publish_future = publisher.publish(topic_path, message_bytes)

        # Wait for the publish to complete
        publish_future.result()

        logging.info(f"Message published to topic {topic_name}")

    except Exception as e:
        logging.error(f"Failed to publish to topic {topic_name}: {e}")
        raise

def bq_load_from_gcs(event, context):
    """Function to handle Pub/Sub events and load data into BigQuery."""
    logging.info(f"Event type: {context.event_type}")
    logging.info(f"Event timestamp: {context.timestamp}")

    try:
        credentials = get_impersonated_credentials()
        bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        logging.info("BigQuery client initialized successfully.")

        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        message_dict = json.loads(pubsub_message)

        bucket = message_dict.get('bucket')
        file_name = message_dict.get('file_name')
        dataset_name = message_dict.get('dataset_name')
        table_name = message_dict.get('table_name')
        logging.info(f"Bucket: {bucket}")
        logging.info(f"File Name: {file_name}")
        logging.info(f"Dataset Name: {dataset_name}")
        logging.info(f"Table Name: {table_name}")

 
        # Check and create the dataset if necessary
        check_and_create_dataset(bq_client, dataset_name)

        # Check and create the table if necessary
        check_and_create_table(bq_client, dataset_name, table_name)

        gcs_uri = f"gs://{bucket}/{file_name}"
        logging.info(f"gcs_uri: {gcs_uri}")

        # Configure the BigQuery load job with auto-detection
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row; adjust to 0 if no header is present
            autodetect=True,  # Enable schema auto-detection
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Use WRITE_TRUNCATE to overwrite, WRITE_APPEND to append
        )

        # Load the data from GCS into BigQuery
        load_job = bq_client.load_table_from_uri(
            gcs_uri, f"{PROJECT_ID}.{dataset_name}.{table_name}", job_config=job_config
        )

        # Waits for the job to complete
        try:
            load_job.result()
            logging.info(f"Job finished. Loaded data from {gcs_uri} into {dataset_name}.{table_name}")
        except Exception as e:
            logging.error(f"Failed to load data from GCS to BigQuery: {e}")
            raise

        message_data = {
        "project": PROJECT_ID,
        "dataset": dataset_name,
        "table": table_name,
        "action": "Data loaded"
        }

        # Attempt to publish the message
        try:
            publish_to_topic(os.getenv('PUBLISH_TOPIC'), message_data)
            logging.info("Message successfully published to Pub/Sub.")
        except Exception as e:
            logging.error(f"Failed to publish message: {e}")

    except Forbidden as e:
        logging.error(f'Error occurred: {str(e)}. Please check the Cloud Function has necessary permissions.')
        raise
