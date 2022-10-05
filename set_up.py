from google.cloud import storage

storage_client = storage.Client()
bucket = storage_client.bucket("bq-data-boost-keys")

blob = bucket.blob("streaming/bq-data-boost-streaming.json")
blob.download_to_filename("streaming-ingestion/bq-data-boost-streaming.json")

blob = bucket.blob("batch/bq-data-boost-batch.json")
blob.download_to_filename("batch-ingestion/bq-data-boost-batch.json")

blob = bucket.blob("raw-data/ny_city_bike_trips.csv")
blob.download_to_filename("batch-ingestion/ny_city_bike_trips.csv")

