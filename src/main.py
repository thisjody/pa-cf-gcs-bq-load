from google.cloud import bigquery, pubsub_v1, secretmanager
from google.oauth2 import service_account
from google.auth.transport import requests
from google.auth import impersonated_credentials
from google.api_core.exceptions import NotFound, Forbidden
import logging
import base64
import json
import os

PROJECT_ID = os.getenv('PROJECT_ID')

def get_secret(secret_name):
    """Retrieve secrets from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    return json.loads(response.payload.data.decode('UTF-8'))

def bq_load_from_gcs(event, context):
    """Function to handle Pub/Sub events and load data into BigQuery."""
    print(f"Event type: {context.event_type}")
    print(f"Event timestamp: {context.timestamp}")

    try:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        message_dict = json.loads(pubsub_message)

        bucket = message_dict.get('bucket')
        file_name = message_dict.get('file_name')
        print(f"Bucket: {bucket}")
        print(f"File Name: {file_name}")

    except Forbidden as e:
        print(f'Error occurred: {str(e)}. Please check the Cloud Function has necessary permissions.')
        raise


