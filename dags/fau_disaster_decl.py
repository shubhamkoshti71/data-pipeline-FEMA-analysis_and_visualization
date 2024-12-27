# Defining the required libraries
from airflow import DAG
from airflow.decorators import dag,task
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import hashlib
import json
import io
from azure.storage.blob import BlobServiceClient



# DAG definition with @dag decorator
@dag(schedule_interval="@daily", start_date=days_ago(1), catchup=False)

def fema_dis_decl():

    #Azure Blob Service Client Setup
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=resource_name;AccountKey=your_auth_key")
    container_name = "container_name"
    container_client = blob_service_client.get_container_client(container_name)

    # API endpoint and pagination settings
    ENDPOINT = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
    LIMIT = 10000
    
    @task(execution_timeout=timedelta(minutes=10))  #allocating sufficient time for the fetch task to extract data
    def fetch():
        """Fetching hazard mitigation data from FEMA API with pagination"""
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
            if 'DisasterDeclarationsSummaries' not in data:
                print("Error: The expected data structure was not found.")
                break

            # Accumulating records
            records = data['DisasterDeclarationsSummaries']
            all_records.extend(records)

            # Break loop if last page reached
            if len(records) < LIMIT:
                break

            # Incrementing skip for pagination
            skip += LIMIT

        return all_records  # Returning all fetched records

    @task
    def upload(records):
        """Hash and upload data to Azure if unique"""
        
        # Serializing the entire dataset and computing hash
        json_bytes = json.dumps(records).encode('utf-8')
        json_buffer = io.BytesIO(json_bytes)
        json_buffer.seek(0)
        data_hash = hashlib.md5(json_bytes).hexdigest()
        snapshot_name = f"disaster_declaration_{data_hash}.json"

        # Checking for existing files to avoid duplicates
        existing_files = [blob.name for blob in container_client.list_blobs()]
        if snapshot_name not in existing_files:
            # Upload new unique snapshot to Azure
            blob_client = container_client.get_blob_client(snapshot_name)
            blob_client.upload_blob(json_buffer, blob_type="BlockBlob")
            print(f"New JSON snapshot '{snapshot_name}' created and uploaded to Azure Blob Storage.")
        else:
            print("No new snapshot created; data is identical to the last upload.")

    # Defining task dependencies
    records = fetch()
    upload(records)

# Instantiating the DAG
fema_disaster_declaration_dag = fema_dis_decl()