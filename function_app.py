import azure.functions as func
import logging
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import os
import io
import pandas as pd
import polars as pl



app = func.FunctionApp()

# @app.blob_trigger(arg_name="myblob", path="project-2/{name}",
#                                connection="fc28c3_STORAGE") 
# def blob_dole(myblob: func.InputStream):
#     logging.info(f"Python blob trigger function processed blob"
#                 f"Name: {myblob.name}"
#                 f"Blob Size: {myblob.length} bytes")





@app.route(route="http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:

    start_time = datetime.now()

    STORAGE_CONNECTION_STRING = (
        "DefaultEndpointsProtocol=http;"
        "AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    )

    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(
        container='project-2',
        blob='npidata_pfile_20050523-20250413.csv'
    )

    # blob_stream = io.BytesIO()
    # blob_client.download_blob().readinto(blob_stream)
    # blob_stream.seek(0)  # Reset stream position

    blob_data = io.BytesIO(blob_client.download_blob().readall())

    df = pl.read_csv(blob_data, infer_schema=False)

    switch_columns=[col for col in df.columns if "Taxonomy Switch_" in col]
    code_columns=[col for col in df.columns if "Taxonomy Code_" in col]
    
    df = df[['NPI',
        'Entity Type Code',
        'Provider Organization Name (Legal Business Name)',
        'Provider Last Name (Legal Name)',
        'Provider First Name',
        'Provider Middle Name',
        'Provider Name Prefix Text',
        'Provider Name Suffix Text',
        'Provider Credential Text',
        'Provider First Line Business Practice Location Address',
        'Provider Second Line Business Practice Location Address',
        'Provider Business Practice Location Address City Name',
        'Provider Business Practice Location Address State Name',
        'Provider Business Practice Location Address Postal Code',
        *switch_columns,
        *code_columns]]
    
    df.write_database(
        table_name="temp_table",
        connection="postgresql://admin:postgres@localhost:5432/project-2",
        if_table_exists="replace"
    )

    logging.info('Python HTTP trigger function processed a request.')

    end_time = datetime.now()

    elapsed = round((end_time - start_time).total_seconds() * 1000)

    print(f"START: {start_time}; END: {end_time};  ELAPSED: {elapsed} ms")
   
    return func.HttpResponse(
            f"Connection successful. Elapsed time: {elapsed} ms",
            status_code=200
    )