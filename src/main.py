from google.cloud import bigquery, pubsub_v1, secretmanager
from google.oauth2 import service_account
from google.auth.transport import requests
from google.auth import impersonated_credentials
from google.api_core.exceptions import NotFound, Forbidden
import logging
import base64
import json
import os

logging.basicConfig(level=logging.INFO)
PROJECT_ID = os.getenv('PROJECT_ID')

def get_secret(secret_name):
    """Retrieve secrets from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    return json.loads(response.payload.data.decode('UTF-8'))

def get_impersonated_credentials():
    """Retrieve impersonated credentials."""
    sa_credentials_secret_name = os.environ.get('SA_CREDENTIALS_SECRET_NAME')
    sa_credentials = get_secret(sa_credentials_secret_name)
    credentials = service_account.Credentials.from_service_account_info(sa_credentials)
    target_principal = os.getenv('IMPERSONATE_SA')
    target_scopes = os.getenv('TARGET_SCOPES').split(",")

    return impersonated_credentials.Credentials(
        source_credentials=credentials,
        target_principal=target_principal,
        target_scopes=target_scopes,
        lifetime=600  # in seconds, optional
    )

def bq_load_from_gcs(event, context):
    """Function to handle Pub/Sub events and load data into BigQuery."""
    logging.info(f"Event type: {context.event_type}")
    logging.info(f"Event timestamp: {context.timestamp}")

    try:
        # Get impersonated credentials and initialize BigQuery client
        credentials = get_impersonated_credentials()
        bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        logging.info("BigQuery client initialized successfully.")
        logging.info(f"BigQuery client: {bq_client}")

        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        message_dict = json.loads(pubsub_message)

        bucket = message_dict.get('bucket')
        file_name = message_dict.get('file_name')
        logging.info(f"Bucket: {bucket}")
        logging.info(f"File Name: {file_name}")

    except Forbidden as e:
        logging.error(f'Error occurred: {str(e)}. Please check the Cloud Function has necessary permissions.')
        raise

