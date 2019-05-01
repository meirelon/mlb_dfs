import os
import re
from datetime import datetime, timedelta

import pandas as pd
import pandas_gbq
from pydfs_lineup_optimizer import get_optimizer, Site, Sport

from deps.gcs import upload_blob, load_pipeline
from deps.draftkings import get_draftkings_players, get_draftkings_predictions
from deps.input import inputData

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


def dk_predictions(request):
    project = os.environ["PROJECT_ID"]
    dataset_base = os.environ["DATASET_BASE"]
    dataset_dfs = os.environ["DATASET_DFS"]
    bucket = os.environ["BUCKET"]
    model_name = os.environ["MODEL"]
    today = (datetime.now() - timedelta(hours=4)).strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    model = load_pipeline(project_id=project,
                          bucket=bucket,
                          destination_path=model_name,
                          filename=model_name)
    input_run = inputData(project=project,
                          dataset=dataset_base,
                          yesterday=yesterday,
                          today=today)
    df = input_run.run()
    df["prediction"] = model.predict(df.drop(["name", "tm"], axis=1))
    prediction_df = df[["name", "tm", "prediction"]]
    pandas_gbq.to_gbq(prediction_df, project_id=project,
              destination_table="{dataset}.mlb_dk_predictions_{dt}".format(dataset=dataset_dfs, dt=today.replace("-","")),
              if_exists="replace")

def dk_lineups(request):
    project = os.environ["PROJECT_ID"]
    bucket = os.environ["BUCKET"]
    dataset_base = os.environ["DATASET_BASE"]
    dataset_dfs = os.environ["DATASET_DFS"]
    today = (datetime.now() - timedelta(hours=4)).strftime('%Y-%m-%d')
    request_json = request.get_json(silent=True)

    if request_json and "n_lineups" in request_json:
        n_lineups = int(request_json.get("n_lineups"))
        if n_lineups > 10:
            n_lineups = 10
    else:
        n_lineups = 2

    df = get_draftkings_predictions(project=project,
                                    dataset_base=dataset_base,
                                    dataset_dfs=dataset_dfs,
                                    dt=today.replace("-",""))
    df.to_csv("/tmp/mlb_dk.csv", index=False)
    optimizer = get_optimizer(Site.DRAFTKINGS, Sport.BASEBALL)
    optimizer.load_players_from_csv("/tmp/mlb_dk.csv")

    lineups = pd.DataFrame()
    cols = ["pos", "first", "last", "position", "team", "opp", "fppg", "salary"]
    i=1
    for lineup in optimizer.optimize(n=n_lineups):
        lineup_list = lineup.printer.print_lineup(lineup).split("\n")[0:10]
        lineup_df = pd.concat([pd.DataFrame(dict(zip(cols,re.split("\s{1,}",x.strip())[1:9])),index=[0])
                               for x in lineup_list], axis=0, ignore_index=True)
        lineup_df["lineup_number"] = i
        i+=1
        lineups = pd.concat([lineups, lineup_df], ignore_index=True)

    lineups.to_csv("/tmp/lineups.csv", index=False)
    upload_blob(bucket_name=bucket,
                source_file_name="/tmp/lineups.csv",
                destination_blob_name="lineups/dk_lineups_{}.csv".format(today.replace("-","")))

    lineup_link = "https://storage.cloud.google.com/{bucket}/lineups/dk_lineups_{dt}.csv"

    return lineup_link.format(bucket=bucket, dt=today.replace("-",""))
