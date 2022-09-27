from azure.storage.blob import BlobClient
import json
import pandas as pd
import pyodbc
from datetime import datetime
import sql
import configparser

parser = configparser.ConfigParser()
parser.read('pipeline.conf')

# Blob container variables
connection_string = parser.get('azure_blob_credentials', 'connection_string')
container_name = parser.get('azure_blob_credentials', 'container_name')
filename = "powerbi_refreshes.json"

# MSSQL database credentials
driver = parser.get('mssql_credentials', 'driver')
server = parser.get('mssql_credentials', 'server')
username = parser.get('mssql_credentials', 'username')
password = parser.get('mssql_credentials', 'password')
database = parser.get('mssql_credentials', 'database')

blob_service_client = BlobClient.from_connection_string(conn_str=connection_string,
                                                        container_name=container_name,
                                                        blob_name=filename)
def connect_to_blob():
    container_client = blob_service_client._get_container_client()
    blob_client = container_client.get_blob_client(filename)
    streamdownloader = blob_client.download_blob()
    fileReader = json.loads(streamdownloader.readall())
    return fileReader

def delete_blob():
    blob_service_client.delete_blob()

def transform_data():
    fileReader = connect_to_blob()
    data = [item for item in fileReader]

    data = pd.read_json(json.dumps(data))
    df = pd.DataFrame(data)

    # Fill refreshing datasets with current time
    df.endTime = df['endTime'].fillna(datetime.utcnow())
    df['startTime'] = pd.to_datetime(df['startTime'], errors='coerce')
    df['endTime'] = pd.to_datetime(df['endTime'], errors='coerce')

    # Replace UTC to Local Time
    df['endTime'] = df['endTime'].dt.tz_convert('Europe/Berlin')
    df['startTime'] = df['startTime'].dt.tz_convert('Europe/Berlin')

    # Replace Unknown status to Running
    df['status'] = df['status'].replace('Unknown', 'Running')

    return df

def load_data():
    # Get dataframe
    df = transform_data()

    # Connect  to SQL Database
    with pyodbc.connect(
        f'Driver={driver};Server={server};Database={database};UID={username};PWD={password}') as conn:
        cursor = conn.cursor()

        # Create Staging table and Target Table
        cursor.execute(sql.create_staging_fact)
        print('Refresh Tables loaded')

        # Insert rows into staging table
        for row in df.itertuples():
            cursor.execute('''
                            DELETE FROM FACT_RefreshHistory_stage 
                            WHERE concat(request_id,status,endtime) 
                            IN (SELECT CONCAT(request_id,status,endtime) 
                                FROM FACT_RefreshHistory)
    
                            INSERT INTO FACT_RefreshHistory_stage 
                            (Dataset_ID, 
                             request_id,
                             refreshType, 
                             startTime, 
                             endtime,
                             status,
                             milestone)
                                VALUES (?,?,?,?,?,?,?)
            ''',
                           row.keys,
                           row.requestId,
                           row.refreshType,
                           row.startTime,
                           row.endTime if row.endTime != "" else 0,
                           row.status,datetime.now())

        # Load data from Staging to Target Table
        cursor.execute(sql.create_fact)
        cursor.execute(sql.insert_fact)
        cursor.execute(sql.drop_stage_fact)

    conn.close()
    print('Data loaded!')
    print('Deleting blob..')
    delete_blob()
    print('Blob deleted!')
