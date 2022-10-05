from google.cloud import pubsub_v1
from utils import user, date, topic_path
from os import environ

pub_sub_credentials = "streaming-ingestion/bq-data-boost-streaming.json"
environ["GOOGLE_APPLICATION_CREDENTIALS"] = pub_sub_credentials

# Initalize a pub/sub client
publisher = pubsub_v1.PublisherClient()

message = "Hello pub/sub message to bigquery"

# date and user are part of the metadata of the message. The data is the message published
publish_job = publisher.publish(topic_path, data=message.encode("utf-8")
, date=date, user=user)
# Wait for the result publish_job
publish_job.result()





