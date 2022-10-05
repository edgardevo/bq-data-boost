import git
import uuid

def get_user()->str:
    reader = git.Repo()
    reader_config = reader.config_reader()
    return reader_config.get_value("user","name")

def rows_in_csv(filename)->int:
    with open(filename) as f:
        return sum(1 for line in f) -1

rows_csv = rows_in_csv("batch-ingestion/ny_city_bike_trips.csv")
user = get_user() or "invalid_user"
project_id = "bq-data-boost"
dataset_id = "batch_ingestion"
table_id = "{project_id}.{dataset_id}.{user}_batch_ingestion".format(user=user, project_id=project_id, dataset_id=dataset_id)
location = "europe-west1"
bucket_name = "bq-data-boost-batch-ingestion-bucket"
batch =uuid.uuid4().hex
destination =user + "/ny_trips/"+batch
uri = "gs://"+bucket_name+"/" + destination

