# bq-data-boost
Big Query Data Boost Material

pip install -r requirements.txt
gcloud config activate sandbox
gsutil cp .local/bq-data-boost-batch.json gs://bq-data-boost-keys/batch-ingestion/bq-data-boost-batch.json
gsutil cp .local/bq-data-boost-streaming.json gs://bq-data-boost-keys/streaming-ingestion/bq-data-boost-streaming.json
