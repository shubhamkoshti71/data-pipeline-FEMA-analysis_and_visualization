# Defining the required libraries
from airflow import DAG
from airflow.decorators import dag,task
from airflow.utils.dates import days_ago
import requests
from datetime import timedelta
import hashlib
import json
import io
from azure.storage.blob import BlobServiceClient



# DAG definition with @dag decorator
@dag(schedule_interval="@daily", start_date=days_ago(1), catchup=False, dagrun_timeout=timedelta(hours=1))

def fema_pub_assist_etl():
    #Azure Blob Service Client Setup
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=resource_name;AccountKey=your_auth_key")
    container_name = "fema-blob-files"
    container_client = blob_service_client.get_container_client(container_name)

    @task(execution_timeout = timedelta(minutes=20))
    def fetch_and_upload():
        """Fetching public assistance data from FEMA API with pagination"""

        # API endpoint and pagination settings
        ENDPOINT = "https://www.fema.gov/api/open/v1/PublicAssistanceFundedProjectsDetails"
        LIMIT = 10000
        skip = 0
        all_records = []
        
        # Retrieving all data with pagination
        while True:
            params = {
                "$top": LIMIT,
                "$skip": skip
            }
            response = requests.get(ENDPOINT, params=params)
            response.raise_for_status()
            data = response.json()

            # Checking if the expected data key is present
            if 'PublicAssistanceFundedProjectsDetails' not in data:
                print("Error: The expected data structure was not found.")
                break

            # Accumulating records
            records = data['PublicAssistanceFundedProjectsDetails']
            all_records.extend(records)

            # Break loop if last page reached
            if len(records) < LIMIT:
                break

            # Incrementing skip for pagination
            skip += LIMIT

        """Hash and upload data to Azure if unique"""
        
        # Serializing entire dataset and compute hash
        json_bytes = json.dumps(all_records).encode('utf-8')
        json_buffer = io.BytesIO(json_bytes)
        json_buffer.seek(0)
        data_hash = hashlib.md5(json_bytes).hexdigest()
        snapshot_name = f"public_assistance_{data_hash}.json"

        # Checking for existing files to avoid duplicates
        existing_files = [blob.name for blob in container_client.list_blobs()]
        if snapshot_name not in existing_files:
            # Upload new unique snapshot to Azure
            blob_client = container_client.get_blob_client(snapshot_name)
            blob_client.upload_blob(json_buffer, blob_type="BlockBlob")
            print(f"New JSON snapshot '{snapshot_name}' created and uploaded to Azure Blob Storage.")
        else:
            print("No new snapshot created; data is identical to the last upload.")


    fetch_and_upload()

# Instantiating the DAG
fema_pub_assistance_dag = fema_pub_assist_etl()
