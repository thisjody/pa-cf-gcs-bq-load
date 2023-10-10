#!/bin/bash

# Check if gcloud command is available
if ! command -v gcloud &> /dev/null; then
    echo "Error: gcloud command not found. Please install the Google Cloud SDK and ensure 'gcloud' is in your PATH."
    exit 1
fi

# Check gcloud authentication
gcloud auth list &> /dev/null
if [ $? -ne 0 ]; then
    echo "Error: Not authenticated to gcloud. Please authenticate using 'gcloud auth login' and ensure you have the necessary permissions."
    exit 1
fi

# Prompt for the .env file to use
echo "Enter the path to the .env file to use (e.g. ./myenvfile.env):"
read ENV_FILE

# Check if the provided .env file exists and is readable
if [[ ! -f "$ENV_FILE" || ! -r "$ENV_FILE" ]]; then
    echo "Error: .env file either does not exist or is not readable."
    exit 1
fi

# Source the .env file
source $ENV_FILE

# Check if required variables are set
declare -a required_vars=("GEN2" "RUNTIME" "REGION" "SERVICE_ACCOUNT" "SOURCE" "ENTRY_POINT" "TRIGGER_TOPIC" "MEMORY" "TIMEOUT" "PROJECT_ID" "IMPERSONATE_SA" "TARGET_SCOPES" "SA_CREDENTIALS_SECRET_NAME")
unset_vars=()

for var in "${required_vars[@]}"; do
    if [[ -z "${!var}" ]]; then
        unset_vars+=("$var")
    fi
done

if [ ${#unset_vars[@]} -ne 0 ]; then
    echo "Error: The following required environment variables are not set: ${unset_vars[*]}"
    exit 1
fi

# Formulate the SET_ENV_VARS value
SET_ENV_VARS="PROJECT_ID=$PROJECT_ID,IMPERSONATE_SA=$IMPERSONATE_SA,TARGET_SCOPES=$TARGET_SCOPES,SA_CREDENTIALS_SECRET_NAME=$SA_CREDENTIALS_SECRET_NAME"

# Deploy the function
gcloud functions deploy pa-cf-gcs-bq-load \
  --$GEN2 \
  --runtime=$RUNTIME \
  --region=$REGION \
  --service-account=$SERVICE_ACCOUNT \
  --source=$SOURCE \
  --entry-point=$ENTRY_POINT \
  --trigger-topic=$TRIGGER_TOPIC \
  --memory=$MEMORY \
  --timeout=$TIMEOUT \
  --set-env-vars "$SET_ENV_VARS"