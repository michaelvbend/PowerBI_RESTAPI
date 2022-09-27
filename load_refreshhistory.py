from azure.storage.blob import BlobClient
import json
import pandas as pd
import pyodbc
from datetime import datetime
import sql

# Blob container variables
def connect_to_blob():
    connection_string = 'BlobEndpoint=https://airflowbigenius.blob.core.windows.net/;QueueEndpoint=https://airflowbigenius.queue.core.windows.net/;FileEndpoint=https://airflowbigenius.file.core.windows.net/;TableEndpoint=https://airflowbigenius.table.core.windows.net/;SharedAccessSignature=sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2022-10-08T16:10:04Z&st=2022-09-16T08:10:04Z&spr=https&sig=7jLaMmVGISJHnaANZYlSyIxN%2BqWZ1xxbQdDeDsFF1s8%3D'
    container_name = 'budget'
    filename = "powerbi_refreshes.json"

    blob_service_client = BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name,
                                                            blob_name=f"powerbi_refreshes.json")
    container_client = blob_service_client._get_container_client()
    blob_client = container_client.get_blob_client(filename)
    streamdownloader = blob_client.download_blob()

    fileReader = json.loads(streamdownloader.readall())
    return fileReader

def delete_blob():
    connection_string = 'BlobEndpoint=https://airflowbigenius.blob.core.windows.net/;QueueEndpoint=https://airflowbigenius.queue.core.windows.net/;FileEndpoint=https://airflowbigenius.file.core.windows.net/;TableEndpoint=https://airflowbigenius.table.core.windows.net/;SharedAccessSignature=sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2022-10-08T16:10:04Z&st=2022-09-16T08:10:04Z&spr=https&sig=7jLaMmVGISJHnaANZYlSyIxN%2BqWZ1xxbQdDeDsFF1s8%3D'
    container_name = 'budget'
    blob_service_client = BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name,
                                                            blob_name=f"powerbi_refreshes.json")
    blob_service_client.delete_blob()

def load_data():
    data = []
    fileReader = connect_to_blob()
    for item in fileReader:
        try:
            for it in item:
                data.append(it)
        except:
            pass
    data = pd.read_json(json.dumps(data))
    df = pd.DataFrame(data)
    # Fill refreshing datasets with current time
    df.endTime = df['endTime'].fillna(datetime.utcnow())
    df['startTime'] = pd.to_datetime(df['startTime'], errors='coerce')
    df['endTime'] = pd.to_datetime(df['endTime'], errors='coerce')

    df['endTime'] = df['endTime'].dt.tz_convert('Europe/Berlin')
    df['startTime'] = df['startTime'].dt.tz_convert('Europe/Berlin')

    # Connect  to SQL Database
    with pyodbc.connect(
        'Driver={SQL Server};Server=bigenius.database.windows.net;Database=powerbi;UID=admin1995;PWD=Koekjes026.') as conn:
        cursor = conn.cursor()
        # Create Staging table and Target Table
        cursor.execute(sql.create_staging_fact)
        print('Refresh Tables loaded')

        # Insert rows into staging table
        for row in df.itertuples():
            cursor.execute('''
                      delete from FACT_RefreshHistory_stage where concat(request_id,status,endtime) in (select concat(request_id,status,endtime) from FACT_RefreshHistory)
    
                    INSERT INTO FACT_RefreshHistory_stage (Dataset_ID, request_id,refreshType, startTime, endtime,status,milestone)
                    VALUES (?,?,?,?,?,?,?)
            ''',
                           row.keys, row.requestId, row.refreshType, row.startTime, row.endTime if row.endTime != "" else 0,row.status,datetime.now())

        # Load data from Staging to Target Table
        cursor.execute(sql.create_fact)
        cursor.execute(sql.insert_fact)
        cursor.execute(sql.drop_stage_fact)

    conn.close()
    print('Data loaded!')

    print('Deleting blob..')
    delete_blob()
    print('Blob deleted!')
