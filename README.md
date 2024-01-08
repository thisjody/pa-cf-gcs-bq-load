# PA-CF-GCS-BQ-LOAD Cloud Function

## Overview

The `pa-cf-gcs-bq-load` Cloud Function is a key component in a data processing pipeline, designed to automate and streamline the process of loading data from Google Cloud Storage (GCS) into Google BigQuery. This function is triggered by events in GCS, specifically targeting file uploads to specified buckets. Upon activation, it performs several critical operations:

1. **Impersonated Credentials**: It employs impersonated credentials for secure access to various Google Cloud services. This function dynamically selects the appropriate service account to impersonate based on the action being performed (e.g., data loading or publishing).

2. **Secret Management**: The function securely retrieves necessary credentials and configurations from the Google Secret Manager, ensuring sensitive information is managed appropriately.

3. **BigQuery Integration with Pandas**: Leveraging the power of pandas for data preprocessing and the `to_gbq` method from `pandas-gbq`, the function seamlessly handles data transformation and efficiently loads data into BigQuery tables. This integration is essential for dealing with complex data structures and ensuring data integrity in the data warehouse.

4. **Dynamic Data Handling and Preprocessing**: The function is capable of processing various types of data, adhering to the formats and schemas as specified in the triggering event. It supports auto-detection of data schemas from the source files and employs pandas for advanced data preprocessing, including handling missing values, type conversions, and sanitizing column names for BigQuery compatibility.

5. **Error Handling and Logging**: It includes robust error handling and logging mechanisms. The function logs detailed information about its operations and any errors encountered, facilitating easier debugging and monitoring. This is extended to include error handling during pandas operations and data loading using `to_gbq`.

6. **Scalability and Flexibility**: Designed with scalability in mind, this function can handle varying loads and data types, making it a flexible solution for different data processing needs within the Google Cloud environment. The integration with pandas enhances its capability to process and transform large datasets efficiently.

The `pa-cf-gcs-bq-load` function exemplifies a serverless approach, allowing for efficient, automated data handling without the need for extensive infrastructure management. By leveraging Google Cloud's capabilities and the power of pandas for data processing, it ensures reliable and secure data processing, catering to the evolving demands of modern data pipelines.

## Utilize the PA-CF Shared Configs Toolkit

For the effective deployment and management of the `pa-cf-gcs-bq-load` Cloud Function, it's vital to check out the [PA-CF Shared Configs Repository](https://github.com/acep-uaf/pa-cf-shared-configs). This toolkit plays a crucial role in standardizing and streamlining the configuration process, ensuring that the cloud function operates seamlessly within the larger cloud infrastructure. Key aspects of the Shared Configs Toolkit include:

1. **Standardized Configuration**: The toolkit provides a set of standardized configurations that are essential for the consistent and error-free deployment of the cloud function.

2. **Efficient Setup**: It simplifies the setup process, reducing the complexity and time required to configure the necessary cloud resources and permissions.

3. **Security and Compliance**: By using predefined configurations, the toolkit helps in maintaining security standards and compliance, ensuring that the cloud function operates within the defined guidelines.

4. **Customization for Specific Needs**: While offering standard configurations, the toolkit also allows for necessary customizations specific to the `pa-cf-gcs-bq-load` function. This includes setting up environment variables, defining roles, and permissions specific to the function's operation.

5. **Simplified Deployment Process**: The toolkit assists in the automated deployment of the cloud function, making it easier to manage and update without extensive manual intervention.

6. **Documentation and Support**: It provides comprehensive documentation and support, aiding in troubleshooting and ensuring that the function is set up correctly.

The integration with the PA-CF Shared Configs Toolkit is vital for ensuring that the `pa-cf-gcs-bq-load` function is deployed efficiently, securely, and in a way that is consistent with the broader data processing infrastructure.

## Prerequisites

Before deploying and utilizing the `pa-cf-gcs-bq-load` Cloud Function, the following prerequisites must be met to ensure successful operation:

1. **Google Cloud SDK**: Installation of the Google Cloud SDK is crucial as it provides the necessary tools for interacting with Google Cloud resources. Ensure that it is properly installed and configured. [Google Cloud SDK Installation Guide](https://cloud.google.com/sdk/docs/install).

2. **Google Cloud Project**: A Google Cloud Project is required where the Cloud Function, GCS, and Pub/Sub are hosted. Have your `PROJECT_ID` readily available for configuration.

3. **Environment Variables**: The function relies on various environment variables for its operation, such as:
    - `PROJECT_ID`: Your Google Cloud Project ID.
    - `PUBLISH_SA`: Service account for publishing operations.
    - `LOAD_SA`: Service account for data loading operations.
    - `SA_CREDENTIALS_SECRET_NAME`: The name of the secret in Google Secret Manager containing the service account credentials.
    - `TARGET_SCOPES`: The required scopes for the impersonated service account.

4. **Google Secret Manager**: It is essential to use Google Secret Manager for storing necessary secrets, like service account credentials, to enhance security.

5. **Google Cloud Storage (GCS)**: The function is designed to react to events in a GCS bucket. Ensure that you have a GCS bucket configured to generate events.

6. **Google Pub/Sub**: Set up a Pub/Sub topic that the function will subscribe to. Ensure the necessary permissions are in place for the function to access and subscribe to this topic.

7. **Permissions**: Adequate permissions are needed for the function and its service account. This includes permissions for accessing the Secret Manager, impersonating other service accounts, and interacting with BigQuery and Pub/Sub.

8. **PA-CF Shared Configs Toolkit**: Familiarity with the PA-CF Shared Configs Toolkit is recommended for efficient setup and deployment.

Completing these prerequisites ensures that the `pa-cf-gcs-bq-load` Cloud Function can be deployed smoothly and will operate as intended in your Google Cloud environment.

## Function Operation

The `pa-cf-gcs-bq-load` Cloud Function operates through a series of steps, orchestrated to efficiently and securely process data from Google Cloud Storage (GCS) into Google BigQuery:

1. **Triggering Mechanism**:
   - The function is indirectly triggered by an event in Google Cloud Storage, specifically the upload of files to a designated bucket.
   - An upstream function, `pa-cf-gcs-event`, is directly activated by this GCS event via Eventarc. It processes the event and then publishes a message to a specific Pub/Sub topic.
   - The `pa-cf-gcs-bq-load` function subscribes to this Pub/Sub topic. When a message is published to the topic, it triggers the execution of `pa-cf-gcs-bq-load`.

2. **Credentials Impersonation**:
   - Upon activation, `pa-cf-gcs-bq-load` identifies the necessary action (such as data loading) and retrieves the appropriate service account credentials for impersonation through the `get_impersonated_credentials` function.
   - This function securely fetches credentials from the Google Secret Manager and creates impersonated credentials based on the required action.

3. **Data Preprocessing with Pandas**:
   - Upon triggering, the function fetches impersonated credentials for 'load' action to securely interact with GCS.
   - It reads the CSV file from GCS into a pandas DataFrame, using the impersonated credentials to access the storage bucket.
   - Data preprocessing includes handling missing values, where 'None', empty strings, and similar representations are converted to NumPy NaN values for consistency and to comply with loading jagged rows into BigQuery allowing for sparse data once the schema has been infered.
   - For columns inferred as strings due to inconsistent data (like whitespaces within numeric values), the function attempts to convert them to numeric types, resorting to NaN in case of conversion failure.

      ```python
      # Replace 'None' strings with numpy NaN
      df.replace(to_replace=['None', 'none', 'NONE', ''], value=np.nan, inplace=True)

      # Remove all spaces within cells for string columns, and convert them to floats if they are numeric
      for col in df.select_dtypes(include=['object']):
          if col != 'time_stamp':  # Skip timestamp column
              df[col] = df[col].str.replace(r"\s+", "", regex=True)  # Remove all spaces
              df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to numeric if possible, otherwise NaN

      # Limit decimal precision to 3 places for float columns
      for col in df.select_dtypes(include=['float']):
          df[col] = df[col].round(3)
      ```
   - Column names are sanitized to be compatible with BigQuery standards. This involves replacing spaces and special characters with underscores, ensuring column names start with a letter or underscore, and truncating them if they are too long.
   
      ```python 
      # Sanitize column names to be BigQuery compatible
      sanitized_columns = []
      for col in df.columns:
          sanitized_col = re.sub(r'[^\w]', '_', col)  # Replace non-alphanumeric characters
          sanitized_col = re.sub(r'__+', '_', sanitized_col)  # Replace multiple underscores
          if not re.match(r'^[a-zA-Z_]', sanitized_col):
              sanitized_col = '_' + sanitized_col  # Prepend underscore if starts with a digit
          sanitized_col = sanitized_col[:128]  # Truncate if too long
          sanitized_columns.append(sanitized_col)
      df.columns = sanitized_columns
      ```


   - Timestamps are parsed correctly, and data types for each column are set appropriately, ensuring data integrity.

4. **BigQuery Client Initialization**:
   - With the impersonated credentials, a BigQuery client is initialized. This client is utilized to manage datasets and tables in BigQuery and to load data into BigQuery tables.
   

5. **Dataset and Table Management**:
   - The function first checks for the existence of the specified dataset in BigQuery, creating it if necessary.
   - It also checks for the specified table within the dataset, creating it with the necessary configuration if it does not exist.

6. **Loading Data into BigQuery using `to_gbq`**:
   - The `to_gbq` method is utilized for loading the processed DataFrame directly into a BigQuery table, using the load impersonated credentials.
   - It also allows for appending data to existing tables without needing to specify the schema each time, making the process efficient and less error-prone.
   - The `if_exists='append'` parameter in `to_gbq` ensures that data is appended to existing tables, facilitating incremental data loads.

7. **Error Handling and Logging**:
   - Throughout its operation, the function logs various informational messages, including the steps being performed and any errors encountered. Robust error handling is implemented to log and raise exceptions, especially during critical operations like dataset/table creation and data loading.

8. **Scalability and Flexibility**:
   - Designed to handle various data formats and volumes, the function is a versatile tool for different data processing tasks within the Google Cloud environment.

This operation flow enables the `pa-cf-gcs-bq-load` Cloud Function to automate data loading processes with a high degree of reliability, security, and scalability, fitting seamlessly into a modern cloud-based data pipeline.

## Functionality Overview

The `pa-cf-gcs-bq-load` Cloud Function plays a pivotal role in the data processing pipeline, offering a range of functionalities:

1. **Automated Data Transfer**: It automates the transfer of data from Google Cloud Storage to Google BigQuery, reducing manual workload and speeding up the data processing pipeline.

2. **Triggered by Pub/Sub**: The function is designed to be triggered by messages on a Google Pub/Sub topic, which are published by an upstream function in response to file uploads in GCS. This ensures a reactive and efficient data processing workflow.

3. **Dynamic Dataset and Table Management**: The function intelligently checks for the existence of datasets and tables in BigQuery and creates them if they are missing. This dynamic management of BigQuery resources simplifies the data handling process.

4. **Scalable Data Processing**: Capable of handling varying data sizes and formats, the function is robust and scalable, suitable for different data processing needs.

5. **Secure Credential Management**: Utilizing Google Secret Manager for credential storage and service account impersonation, the function maintains high security standards for data access and manipulation.

6. **Error Handling and Logging**: Comprehensive error handling is built-in, with detailed logging for each operation step. This approach enhances the reliability and maintainability of the function.

7. **BigQuery Integration**: It seamlessly integrates with BigQuery, using optimized methods for data loading and ensuring data integrity and consistency.

The `pa-cf-gcs-bq-load` function stands out as a reliable, secure, and efficient solution for automating data flow from GCS to BigQuery, making it a valuable asset in the cloud data processing ecosystem.

## Enhanced Error Handling Mechanisms

The `pa-cf-gcs-bq-load` Cloud Function incorporates robust error handling mechanisms throughout its operation, ensuring reliable processing of data from Google Cloud Storage to BigQuery. This section outlines the error handling strategies employed during different stages of the function:

### During Pandas Data Preprocessing

1. **Data Type Conversion and Sanitization**:
   - The function attempts to convert string columns to numeric types where appropriate, replacing non-numeric characters and handling missing values (`NaN`).
   - Errors encountered during this conversion process, such as inability to convert a particular string to a float due to format inconsistencies, are logged for review and diagnosis.
   - Data sanitization involves removing unwanted characters and standardizing column names to meet BigQuery's requirements. Any issues encountered here are logged, and the function employs strategies to correct them wherever possible.

2. **Timestamp Parsing**:
   - The function parses the `time_stamp` column to ensure it's in the correct format for BigQuery. Errors in timestamp parsing are logged and handled to prevent data loading issues.

### While Using to_gbq for Data Loading

1. **Loading Process Handling**:
   - The `to_gbq` method dynamically handles schema inconsistencies and manages the loading of data into BigQuery.
   - The function logs detailed errors related to connectivity issues, permission problems, or BigQuery API exceptions during the loading process.
   - The usage of impersonated credentials ensures that the function operates with the necessary permissions and access scopes required for data loading.

2. **Handling Sparse Data**:
   - The function is capable of handling cases where incoming data might be sparse or have missing columns. The `to_gbq` method manages these scenarios by aligning the data with the existing BigQuery table schema.

### General Error Handling

1. **BigQuery Client Initialization**:
   - Errors encountered during the initialization of the BigQuery client, such as issues with impersonated credentials or connectivity problems, are logged for further action.

2. **Dataset and Table Creation**:
   - The function checks for the existence of the specified dataset and table in BigQuery and attempts to create them if they don't exist. Errors during this process, like permission denials or API failures, are logged.

3. **Pub/Sub Message Processing**:
   - Errors in processing Pub/Sub messages, such as issues in message decoding or JSON parsing, are captured and logged.

4. **Comprehensive Logging and Error Propagation**:
   - The function logs all errors with detailed information, aiding in troubleshooting and maintenance.
   - In scenarios where the function cannot resolve errors internally, it propagates them for external monitoring and alerting.
  
6. **Secure Failure State**: 
    - In scenarios where continued operation could lead to data inconsistency or other critical issues, the   function is designed to fail securely. This approach prioritizes data integrity and system stability.

5. **Alerting and Monitoring Integration**: 
    - The function's error handling integrates with Google Cloud's monitoring and alerting systems, enabling real-time notifications and in-depth analysis of error conditions.

Through these enhanced error handling mechanisms, the `pa-cf-gcs-bq-load` Cloud Function ensures a robust and resilient operation, effectively managing various types of errors and exceptions that may occur during data processing and loading.

## Dependencies

The `pa-cf-gcs-bq-load` Cloud Function relies on several external dependencies to ensure smooth and efficient operation. These dependencies include various Python packages and Google Cloud services. Here is a list of the key dependencies:

1. **Google Cloud BigQuery**: Used for managing and interacting with BigQuery datasets and tables, and for loading data into BigQuery.

2. **Google Cloud Storage**: Essential for accessing and handling files stored in Google Cloud Storage buckets.

3. **Google Cloud Pub/Sub**: For subscribing to Pub/Sub topics and processing messages that trigger the function.

4. **Google Secret Manager**: Utilized for securely managing and accessing sensitive information like service account credentials.

5. **Google Cloud SDK**: Provides the necessary tools for interacting with Google Cloud resources.

6. **Python Libraries**:
    - `google-cloud-bigquery`: The Python client library for Google BigQuery.
    - `google-cloud-storage`: The Python client library for Google Cloud Storage.
    - `google-cloud-pubsub`: The Python client library for Google Cloud Pub/Sub.
    - `google-auth`: Provides authentication and authorization functionalities.
    - `google-auth-httplib2` and `google-auth-oauthlib`: Support libraries for `google-auth`.
    - `google-cloud-secret-manager`: The Python client library for accessing secrets stored in Google Secret Manager.
    - `pandas`: Essential for data manipulation and preprocessing before loading into BigQuery.
    - `numpy`: Supporting `pandas` for numerical operations.
    - `pandas_gbq`: A critical library used for loading data from pandas DataFrames directly into Google BigQuery.

    These libraries provide authentication functionalities, essential for interacting securely with Google Cloud services and are listed in `requirements.txt` .

7. **Logging and Error Handling Tools**:
    - `logging`: A standard Python library used for logging events and errors.

It's important to ensure that these dependencies are installed and properly configured before deploying and running the `pa-cf-gcs-bq-load` Cloud Function. This will help in avoiding runtime errors and ensure the function operates as expected.

## Roles and Service Accounts

The `PA-CF-GCS-BQ-LOAD` Cloud Function efficiently utilizes configurations from the [PA-CF Shared Configs Toolkit](https://github.com/acep-uaf/pa-cf-shared-configs) for managing service accounts. This approach ensures standardized, secure, and efficient deployment and operation within the cloud environment.

### Service Accounts

The function employs three principal service accounts, each serving a distinct role in the function's lifecycle and operations:

1. **Deploy Service Account (`$SERVICE_ACCOUNT`)**:
   - Responsible for the initial deployment of the Cloud Function.
   - After deployment, it primarily functions to impersonate other service accounts (`PUBLISH_SA` and `LOAD_SA`) for specific tasks, thereby adhering to the principle of least privilege.
   - The exact configuration of this service account is managed through the shared configs, ensuring secure and controlled deployment processes.

2. **Publish Service Account (`$PUBLISH_SA`)**:
   - Used for publishing operations, particularly for sending messages to Google Cloud Pub/Sub.
   - Its setup and permissions are defined within the shared configs, aligning with organizational security standards and operational requirements.

3. **Load Service Account (`$LOAD_SA`)**:
   - Specialized for data loading tasks, especially involving operations with Google BigQuery.
   - The configuration of this service account, including its permissions, is also managed via the shared configs to ensure appropriate access controls.

By leveraging the PA-CF Shared Configs Toolkit, these service accounts are configured to ensure that the `PA-CF-GCS-BQ-LOAD` Cloud Function operates securely, efficiently, and in compliance with best practices. For detailed information on these service accounts' configurations, refer to the [PA-CF Shared Configs Toolkit](https://github.com/acep-uaf/pa-cf-shared-configs).

## Deployment Script for PA-CF-GCS-BQ-LOAD Cloud Function

The `pa_cf_gcs_bq_load_deploy.sh` script streamlines the deployment of the `pa-cf-gcs-bq-load` Cloud Function. This script automates the deployment process, ensures that the necessary prerequisites are met, and correctly sets up the environment variables required for the function's operation.

## Usage
To use the script, execute the following command:

```bash
./pa_cf_gcs_bq_load_deploy.sh
```
## Workflow

1. **gcloud command check**: The script first checks if the `gcloud` command-line tool is available on the system. If not, an error is raised.

```bash
if ! command -v gcloud &> /dev/null; then
    ...
fi
```

2. **gcloud authentication check**: The script then checks if the user is authenticated with `gcloud`.

```bash
gcloud auth list &> /dev/null
...
```

3. **.env file prompt**: The user is prompted to provide the path to an `.env` file that contains required environment variables.

```bash
echo "Enter the path to the .env file to use (e.g. ./myenvfile.env):"
read ENV_FILE
```

4. **.env file verification**: It checks if the provided .env file exists and is readable.

```bash
if [[ ! -f "$ENV_FILE" || ! -r "$ENV_FILE" ]]; then
    ...
fi
```

5. **Source .env file**: The .env file's variables are loaded into the script's environment.

```bash
source $ENV_FILE
```

6. **Environment Variable Checks**: The script checks if all the required environment variables are set. If any are missing, the deployment will not proceed.

```bash
declare -a required_vars=("GEN2" "RUNTIME" ... "SA_CREDENTIALS_SECRET_NAME")
...
```

7. **Cloud Function Deployment**: With all checks complete and the environment set up, the script deploys the Cloud Function using the `gcloud command`.

```bash
gcloud functions deploy $CLOUDFUNCTION \
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
  ```

## .env File Configuration
You should have an `deploy.env` file with the following variables defined:

```bash
GEN2=<value>
RUNTIME=<value>
REGION=<value>
SERVICE_ACCOUNT=<value>
CLOUDFUNCTION=<value>
SOURCE=<value>
ENTRY_POINT=<value>
TRIGGER_TOPIC=<value>
PUBLISH_TOPIC=<value>
MEMORY=<value>
TIMEOUT=<value>
PROJECT_ID=<value>
PUBLISH_SA=<value>
LOAD_SA=<value>
TARGET_SCOPES=<value>
SA_CREDENTIALS_SECRET_NAME=<value>
```

Replace `<value>` with the appropriate values for your deployment.

## Environment Variable Descriptions
Below are descriptions for each environment variable used in the deployment script:

- **GEN2**=`<value>`:
  - Description: Specifies the generation of the Cloud Function to deploy. For example: `gen2` when you intend to deploy a second generation Google Cloud Function.

- **RUNTIME**=`<value>`:
  - Description: Specifies the runtime environment in which the Cloud Function executes. For example: `python311` for Python 3.11.

- **REGION**=`<value>`:
  - Description: The Google Cloud region where the Cloud Function will be deployed and run. Example values are `us-west1`, `europe-west1`, etc.

- **SERVICE_ACCOUNT**=`<value>`:
  - Description: The service account under which the Cloud Function will run. This defines the permissions that the Cloud Function has during execution.

- **CLOUDFUNCTION**=`<value>`:
   - Description: The name of the Cloud Function to be deployed. It is used in the deployment script to identify which function the `gcloud functions deploy` command will target. 

- **SOURCE**=`<value>`:
  - Description: Path to the source code of the Cloud Function. Typically, this points to a directory containing all the necessary files for the function.

- **ENTRY_POINT**=`<value>`:
  - Description: Specifies the name of the function or method within the source code to be executed when the Cloud Function is triggered.

- **TRIGGER_TOPIC**=`<value>`:
   - Description: The name of the Pub/Sub topic from which the Cloud Function will be triggered.

- **$PUBLISH_TOPIC**=`<value>`:
   - Description: The name of the Pub/Sub topic to which the Cloud Function will publish messages.

- **MEMORY**=`<value>`:
  - Description: The amount of memory to allocate for the Cloud Function. This is denoted in megabytes, e.g., `16384MB`.

- **TIMEOUT**=`<value>`:
  - Description: The maximum duration the Cloud Function is allowed to run before it is terminated. Expressed in seconds, e.g., `540s`.

- **PROJECT_ID**=`<value>`:
  - Description: The Google Cloud Project ID where the Cloud Function, GCS, and Pub/Sub reside.

- **PUBLISH_SA**=`<value>`:
  - Description: This is the service account used for publishing operations within the Cloud Function. It is designed to have specific permissions necessary for publishing data to Google Pub/Sub or other similar services. This account should have the appropriate roles and permissions for these tasks.

- **LOAD_SA**=`<value>`:
  - Description: This service account is used for data loading operations. It is specifically configured to interact with Google BigQuery and Google Cloud Storage for the purpose of loading data into BigQuery. It should have the necessary roles and permissions to access GCS, manage BigQuery datasets and tables, and perform data load operations.

- **TARGET_SCOPES**=`<value>`:
  - Description: The authentication scopes required for the impersonated service account. Example: `https://www.googleapis.com/auth/cloud-platform` for a full access scope to GCP services.

- **SA_CREDENTIALS_SECRET_NAME**=`<value>`:
  - Description: The name of the secret stored in Google Secret Manager that contains the service account credentials used by the Cloud Function.

## Conclusion

The `PA-CF-GCS-BQ-LOAD` Cloud Function stands as a testament to the power of cloud automation in the Google Cloud environment. This function streamlines the process of transferring data from Google Cloud Storage to BigQuery, significantly accelerating data processing tasks while maintaining a high level of accuracy and security.

Key features such as automated data transfer, flexible data handling, and robust error management underscore the function's critical role in data processing pipelines. The incorporation of advanced data preprocessing capabilities using Pandas adds another layer of efficiency and reliability. With Pandas, the function is able to handle a wider range of data formats and complexities, ensuring the data loaded into BigQuery is optimized for analysis.

Moreover, the utilization of the PA-CF Shared Configs Toolkit enhances the function's deployment and operational efficiency. This ensures scalability and compliance with best practices, making it a flexible and robust solution for various data processing needs.

The `PA-CF-GCS-BQ-LOAD` Cloud Function is ideal for organizations seeking to optimize their cloud data workflows. It embodies the principles of modern cloud computing and data handling, making it a compelling choice for efficient and secure cloud-based data management. Its implementation marks a crucial step towards streamlined, effective, and secure management of data in the cloud environment.




