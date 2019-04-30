import os
import pandas as pd
import pandas_gbq
from deps.gcs import upload_blob
from deps.draftkings import get_draftkings_players
from datetime import datetime, timedelta

def file_to_gcs(request):
    """
    Accepts a json payload for HTTP POST
    """
    bucket = os.environ["BUCKET"]
    today = (datetime.now() - timedelta(hours=4)).strftime('%Y-%m-%d')

    request_json = request.get_json(silent=True)
    if request_json and "dk" in request_json:
        df = pd.read_csv(request_json.get("dk"))
        df.to_csv("/tmp/dk.csv", index=False)
        upload_blob(bucket_name=bucket,
                    source_file_name="/tmp/dk.csv",
                    destination_blob_name="mlb_{}.csv".format(today.replace("-","")))


def dk_to_gcp(request):
    """
    No json payload- simply makes HTTP GET and returns csv
    """
    project = os.environ["PROJECT_ID"]
    dataset = os.environ["DATASET"]
    bucket = os.environ["BUCKET"]
    today = (datetime.now() - timedelta(hours=4)).strftime('%Y-%m-%d')

    df = get_draftkings_players()
    # df.to_csv("/tmp/dk.csv", index=False)
    # upload_blob(bucket_name=bucket,
    #             source_file_name="/tmp/dk.csv",
    #             destination_blob_name="data/mlb_{}.csv".format(today.replace("-","")))

    pandas_gbq.to_gbq(df, project_id=project,
              destination_table="{dataset}.mlb_draftkings_{dt}".format(dataset=dataset, dt=today.replace("-","")),
              if_exists="replace")
