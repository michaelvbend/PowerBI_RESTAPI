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
filename = "powerbi_workspace.json"

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
    data = [item for item in fileReader['value']]

    data = pd.read_json(json.dumps(data))
    df = pd.DataFrame(data)

    return df


def load_data():
    # Get dataframe
    df = transform_data()

    # Connect  to SQL Database
    with pyodbc.connect(
            f'Driver={driver};Server={server};Database={database};UID={username};PWD={password}') as conn:
        cursor = conn.cursor()

        # Create Staging table and Target Table
        cursor.execute(sql.create_staging_dim_workspace)
        print('Workspace Tables loaded')

        # Insert rows into staging table
        for row in df.itertuples():
            cursor.execute('''
                            INSERT INTO DIM_Workspace_stage 
                            (Workspace_ID, 
                             Workspace_name
                                )
                                VALUES (?,?)
            ''',
                           row.id,
                           row.name
                           )

        # Load data from Staging to Target Table
        cursor.execute(sql.create_dim_workspace)
        cursor.execute(sql.insert_dim_workspace)
        cursor.execute(sql.drop_stage_dim_workspace)

    conn.close()
    print('Data loaded!')
    print('Deleting blob..')
    delete_blob()
    print('Blob deleted!')
