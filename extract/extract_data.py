from powerbi import PowerBIAPI
from azure.storage.blob import BlobClient
import json
import configparser

parser = configparser.ConfigParser()
parser.read('pipeline.conf')

# Blob container variables
connection_string = parser.get('azure_blob_credentials', 'connection_string')
container_name = parser.get('azure_blob_credentials', 'container_name')

# Power BI credentials
tenant = parser.get('powerbi_credentials', 'tenant')
username = parser.get('powerbi_credentials', 'username')
password = parser.get('powerbi_credentials', 'password')
client_id = parser.get('powerbi_credentials', 'client')
client_secret = parser.get('powerbi_credentials', 'client_secret')

# Extract dataset data & refresh history. Upload it directly to Blob Storage after retrieval
def extract_data():
    # Connect to Power BI REST API
    client = PowerBIAPI(tenant=tenant, username=username,
                        password=password, client=client_id,
                        client_secret=client_secret)

    categories = ['datasets', 'refreshes', 'workspace']

    for cat in categories:
        blob = BlobClient.from_connection_string(conn_str=connection_string,
                                                 container_name=container_name,
                                                 blob_name=f"powerbi_{cat}.json")
        try:
            print(f"Uploading blob {cat}...")
            # Skip if blob already exists
            if cat == "datasets" and blob.exists() is False:
                blob.upload_blob(json.dumps(client.getDatasets()))
            elif cat == "workspace" and blob.exists() is False:
                blob.upload_blob(json.dumps(client.getGroups()))
            elif cat == "refreshes" and blob.exists() is False:
                blob.upload_blob(json.dumps(client.getRefreshHistory()))

            print("Succes!")
        except Exception as e:
            raise Exception







