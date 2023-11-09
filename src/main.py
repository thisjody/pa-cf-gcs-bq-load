from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
from google.auth import impersonated_credentials
from google.api_core.exceptions import NotFound, Forbidden
import logging
import base64
import json
import os

logging.basicConfig(level=logging.INFO)
PROJECT_ID = os.getenv('PROJECT_ID')

IMPERSONATE_SA_MAP = {
    'publish': os.environ['PUBLISH_SA'],
    'load': os.environ['LOAD_SA']
}

def get_secret(secret_name):
    """Retrieve secrets from Google Secret Manager."""
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

def check_and_create_table(bq_client, dataset_name, table_name, schema):
    """Check if a table exists within the dataset, if not create it with the provided schema."""
    table_id = f"{PROJECT_ID}.{dataset_name}.{table_name}"
    try:
        table = bq_client.get_table(table_id)  # Make an API request.
        logging.info(f"Table {table_name} already exists in dataset {dataset_name}.")
    except NotFound as e:
        # This is an expected exception in case the table does not exist, so INFO level may be more appropriate
        logging.info(f"Table not found, which is expected if it does not exist yet: {e}")
        
        logging.info(f"Table {table_name} not found in dataset {dataset_name}. Creating it now.")
        table = bigquery.Table(table_id, schema=schema)
        # TODO: Set additional table properties, such as time partitioning, here if necessary.
        try:
            created_table = bq_client.create_table(table, timeout=30)  # Make an API request.
            logging.info(f"Created table {created_table.project}.{created_table.dataset_id}.{created_table.table_id}")
        except Exception as e:
            # If there's an error in creating the table, log that exception as well.
            logging.error(f"Failed to create table: {e}")
            raise

def publish_to_topic(data):
    """Placeholder function for publishing data to a yet-to-be-defined topic."""
    # This function will be implemented later to publish data to a topic
    pass

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

        # Define the schema for the table.
        schema = [
            bigquery.SchemaField("alpha", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("beta", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("gamma", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("delta", "STRING", mode="NULLABLE"),
        ]

        # Check and create the table if necessary.
        check_and_create_table(bq_client, dataset_name, table_name, schema)

        gcs_uri = f"gs://{bucket}/{file_name}"
        logging.info(f"gcs_uri: {gcs_uri}")




        # Placeholder: Call the publish function when ready
        # publish_to_topic(your_data_here)

    except Forbidden as e:
        logging.error(f'Error occurred: {str(e)}. Please check the Cloud Function has necessary permissions.')
        raise
