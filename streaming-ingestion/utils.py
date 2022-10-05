import git
from datetime import datetime

def get_user()->str:
    reader = git.Repo()
    reader_config = reader.config_reader()
    return reader_config.get_value("user","name")

user = get_user()
date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
project_id = "bq-data-boost"
topic_name = "streamingtopic"
topic_path = "projects/{project_id}/topics/{topic_name}".format(project_id=project_id, topic_name=topic_name)
