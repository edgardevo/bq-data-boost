from os import environ
from google.cloud import bigquery, storage
from utils import table_id, location,bucket_name, destination, uri, rows_csv


bq_gcs_credentials = "batch-ingestion/bq-data-boost-batch.json"
environ["GOOGLE_APPLICATION_CREDENTIALS"] = bq_gcs_credentials
table_suffix = "_suffix_upload_batch"

def upload_blob(bucket_name:str, source_file_name:str, destination_blob_name:str)->None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def ingest_to_bigquery(table_suffix:str, uri:str)->None:
    full_table_id = table_id+table_suffix
    bq_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    job = bq_client.load_table_from_uri(
        uri,
        full_table_id,
        location=location,
        job_config=job_config,
    )

    job.result()

def get_number_of_rows(table_suffix) -> int:
    full_table_id = table_id+table_suffix
    sql_query = (
                f"SELECT count(*) FROM {full_table_id}"
            )
    bq_client = bigquery.Client()
    query_job = bq_client.query(sql_query)
    result = query_job.result()
    row = next(result)
    return row[0] or 0

print(f"Uploading a CSV file to {destination} in bucket {bucket_name}")
upload_blob(bucket_name, "batch-ingestion/ny_city_bike_trips.csv", destination)

print(f"Creating BigQuery table {table_id}{table_suffix} from gcs bucket {uri}")
ingest_to_bigquery(table_suffix, uri)

print(f"Testing table creation")
number_of_rows_in_bq = get_number_of_rows(table_suffix)
print(f"Table {table_id}{table_suffix} contains {number_of_rows_in_bq} rows")
if number_of_rows_in_bq != rows_csv:
    raise Exception(f"Number of rows {number_of_rows_in_bq} in BigQuery doesn't match number of rows in the CSV file {rows_csv}")

print("Succesfully create table in bigquery {table_id}{table_suffix} from {destination}")
