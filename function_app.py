import azure.functions as func
import logging
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import os
import io
import polars as pl
import psycopg2



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
        blob='nppes_raw.parquet'
        # blob='npidata_pfile_20050523-20250413.csv'
    )

    blob = blob_client.download_blob()
    blob_stream = io.BytesIO(blob.readall())

    df = pl.read_parquet(blob_stream)
    
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

    RAW_DATA_TABLE = 'staging_raw'
    CLEAN_DATA_PROC = 'clean_data'
    COMPILE_DATA_PROC = 'compile_data'
    STORE_DATA_PROC = 'store_data'
    BATCH_SIZE = 300000
    COMMIT_SIZE = df.shape[0]

    with psycopg2.connect(
        dbname="project-2",
        user="admin",
        password="password",
        host="localhost",
        port="5432"
    ) as conn:
        with conn.cursor() as cursor:
            offset = 0
            batch_counter = []

            while offset < COMMIT_SIZE:
                batch_start = datetime.now()

                end_index = min(offset + BATCH_SIZE, COMMIT_SIZE)

                df_as_csv = df[offset:end_index].write_csv()
                
                cursor.execute(f'TRUNCATE TABLE {RAW_DATA_TABLE};')

                cursor.copy_expert(
                    f"COPY {RAW_DATA_TABLE} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                    io.StringIO(df_as_csv)
                )

                cursor.execute(f'CALL {CLEAN_DATA_PROC}();')
                cursor.execute(f'CALL {COMPILE_DATA_PROC}();')
                cursor.execute(f'CALL {STORE_DATA_PROC}();')

                conn.commit()

                batch_end = datetime.now()
                batch_counter.append(round((batch_end - batch_start).total_seconds() * 1000))

                offset += BATCH_SIZE

    end_time = datetime.now()

    elapsed = round((end_time - start_time).total_seconds() * 1000)

    print(f"START: {start_time}; END: {end_time};  ELAPSED: {elapsed} ms")
   
    return func.HttpResponse(
            f"Connection successful / Elapsed time: {elapsed} ms / Batch times: {batch_counter} / Average batch time: {round(sum(batch_counter) / len(batch_counter))} ms / Total batch time: {round(sum(batch_counter))} ms",
            status_code=200
    )
