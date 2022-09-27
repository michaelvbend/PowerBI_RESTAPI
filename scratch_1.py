from test import PowerBIAPI
from azure.storage.blob import BlobClient
import json


# # Blob container variables
connection_string = 'BlobEndpoint=https://airflowbigenius.blob.core.windows.net/;QueueEndpoint=https://airflowbigenius.queue.core.windows.net/;FileEndpoint=https://airflowbigenius.file.core.windows.net/;TableEndpoint=https://airflowbigenius.table.core.windows.net/;SharedAccessSignature=sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2022-10-08T16:10:04Z&st=2022-09-16T08:10:04Z&spr=https&sig=7jLaMmVGISJHnaANZYlSyIxN%2BqWZ1xxbQdDeDsFF1s8%3D'
container_name = 'budget'

# Get all Power BI datasets in workspace and append to list
def get_Datasets():
    client = PowerBIAPI(tenant='8db24b73-51eb-42db-9b0e-dfbfa38c5d27', username='test@bi-genius.nl',
                        password='Lopen100', client='47fb6774-dff7-4199-b431-7688b08f0f71',
                        client_secret='Lnj8Q~.oHaCNFf4sOvox5ROThgQYG56.fW4wWbPX')

    list_of_datasets = []
    datasets = client.getDatasets()['value']
    for item in datasets:
        list_of_datasets.append(item['id'])

    return list_of_datasets

def extract_data(datasets: "function"):
    client = PowerBIAPI(tenant='8db24b73-51eb-42db-9b0e-dfbfa38c5d27', username='test@bi-genius.nl',
                        password='Lopen100', client='47fb6774-dff7-4199-b431-7688b08f0f71',
                        client_secret='Lnj8Q~.oHaCNFf4sOvox5ROThgQYG56.fW4wWbPX')

    datasets = datasets
    categories = ['datasets', 'refreshes']
    refreshes = []
    for dataset in datasets:
        refreshes.append(client.getRefreshHistory(dataset))

    for cat in categories:
        blob = BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name,
                                                         blob_name=f"powerbi_{cat}.json")
        try:
            print(f"Uploading blob {cat}...")

            if cat == "datasets":
                # blob.upload_blob(json.dumps(client.getDatasets()))
                blob.upload_blob(json.dumps(client.getDatasets()))
            elif cat == "refreshes":
                blob.upload_blob(json.dumps(refreshes))

            print("Succes!")
        except Exception as e:
            print(str(e))
            raise









