from azure.storage.blob import BlobClient
import json
import pandas as pd
import pyodbc
import sql
# Blob container variables
def connect_to_blob():
    connection_string = 'BlobEndpoint=https://airflowbigenius.blob.core.windows.net/;QueueEndpoint=https://airflowbigenius.queue.core.windows.net/;FileEndpoint=https://airflowbigenius.file.core.windows.net/;TableEndpoint=https://airflowbigenius.table.core.windows.net/;SharedAccessSignature=sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2022-10-08T16:10:04Z&st=2022-09-16T08:10:04Z&spr=https&sig=7jLaMmVGISJHnaANZYlSyIxN%2BqWZ1xxbQdDeDsFF1s8%3D'
    container_name = 'budget'
    filename = "powerbi_datasets.json"

    blob_service_client = BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name,blob_name=f"powerbi_datasets.json")
    container_client = blob_service_client._get_container_client()
    blob_client = container_client.get_blob_client(filename)
    streamdownloader = blob_client.download_blob()

    fileReader = json.loads(streamdownloader.readall())
    return fileReader

def delete_blob():
    connection_string = 'BlobEndpoint=https://airflowbigenius.blob.core.windows.net/;QueueEndpoint=https://airflowbigenius.queue.core.windows.net/;FileEndpoint=https://airflowbigenius.file.core.windows.net/;TableEndpoint=https://airflowbigenius.table.core.windows.net/;SharedAccessSignature=sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2022-10-08T16:10:04Z&st=2022-09-16T08:10:04Z&spr=https&sig=7jLaMmVGISJHnaANZYlSyIxN%2BqWZ1xxbQdDeDsFF1s8%3D'
    container_name = 'budget'
    blob_service_client = BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name,
                                                            blob_name=f"powerbi_datasets.json")
    blob_service_client.delete_blob()

def load_data():
    fileReader = connect_to_blob()
    data = pd.read_json(json.dumps(fileReader['value']))

    df = pd.DataFrame(data)
    df['description'] = df['description'].fillna('Geen beschrijving' )

    conn = pyodbc.connect('Driver={SQL Server};Server=bigenius.database.windows.net;Database=powerbi;UID=admin1995;PWD=Koekjes026.')
    cursor = conn.cursor()

    # Create Staging table and Target Table
    cursor.execute(sql.create_staging_dim)
    cursor.execute(sql.create_dim)
    print('Dataset Tables loaded')
    conn.commit()

    for row in df.itertuples():
        cursor.execute('''
                delete from DIM_Dataset_stage where Dataset_ID in (select Dataset_ID from DIM_Dataset)
                INSERT INTO DIM_Dataset_stage (Dataset_ID, Dataset_Name,Created_date, Description, Dataset_owner)
                VALUES (?,?,?,?,?)
        ''',
                       row.id,row.name,row.createdDate,row.description, row.configuredBy)
    conn.commit()

    # Load data from Staging to Target Table
    cursor.execute(sql.insert_dim)
    cursor.execute(sql.drop_stage_dim)
    conn.commit()
    print('Data loaded!')


    conn.close()
    print('Deleting blob..')
    delete_blob()
    print('Blob deleted!')


