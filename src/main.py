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

def bq_load_from_gcs(event, context):
    print(f'Received event: {event}')  
    logging.info(f'Received event: {event}')
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
