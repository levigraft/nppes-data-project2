import azure.functions as func
import logging
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import os
import io
import polars as pl
import psycopg2
import requests



app = func.FunctionApp()

@app.route(route="http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:

    start_time = datetime.now()

    STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")

    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(
        container=os.getenv("BLOB_CONTAINER_NAME"),
        blob=os.getenv("BLOB_NAME")
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
        dbname=os.getenv("DATABASE_NAME"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        host=os.getenv("DATABASE_HOST"),
        port=os.getenv("DATABASE_PORT")
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

@app.route(route="update_zip_populations", auth_level=func.AuthLevel.ANONYMOUS)
def update_zip_populations(req: func.HttpRequest) -> func.HttpResponse:

    STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")

    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    
    zips_blob_client = blob_service_client.get_blob_client(
        container=os.getenv("BLOB_CONTAINER_NAME"),
        blob="ZIP_COUNTY_032025.xlsx"
    )

    zips_stream = io.BytesIO(zips_blob_client.download_blob().readall())

    zips_data = pl.read_excel(zips_stream, engine='openpyxl')

    url = 'https://api.census.gov/data/2023/acs/acs5?get=NAME,B01001_001E&for=county:*'
    response = requests.get(url)

    if response.status_code == 200:
        pop_data = response.json()
        pop_df = pl.DataFrame(pop_data[1:], schema=['name', 'population', 'state', 'county'])
        
        pop_df = pop_df.with_columns(
            pl.col("name").str.split(", ").alias("split_name")
        )

        pop_df = pop_df.with_columns([
            pl.col("split_name").list.get(0).alias("county_name"),
            pl.col("split_name").list.get(1).alias("state_name")
        ])

        # Create 'fips_code' and cast 'population' to int
        pop_df = pop_df.with_columns([
            (pl.col("state") + pl.col("county").str.zfill(3)).alias("fips_code"),
            pl.col("population").cast(pl.Int64)
        ])

        with psycopg2.connect(
                dbname=os.getenv("DATABASE_NAME"),
                user=os.getenv("DATABASE_USER"),
                password=os.getenv("DATABASE_PASSWORD"),
                host=os.getenv("DATABASE_HOST"),
                port=os.getenv("DATABASE_PORT")
            ) as conn:
                with conn.cursor() as cursor:
                    
                    zips_as_csv = zips_data[["ZIP", "COUNTY", "USPS_ZIP_PREF_CITY", "USPS_ZIP_PREF_STATE"]].write_csv()
                        
                    cursor.execute(f'TRUNCATE TABLE zip_code_data;')

                    cursor.copy_expert(
                        f"COPY zip_code_data FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                        io.StringIO(zips_as_csv)
                    )

                    pop_as_csv = pop_df[["fips_code", "population", "county_name", "state_name"]].write_csv()

                    cursor.execute(f'TRUNCATE TABLE county_population;')

                    cursor.copy_expert(
                        f"COPY county_population FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                        io.StringIO(pop_as_csv)
                    )

                    conn.commit()

        return func.HttpResponse(
            f"Zip code and population data updated.",
            status_code=200)
    else:
        raise Exception(f"API request failed: {response.status_code}")
