import os
import pandas as pd
from deps.gcs import upload_blob
from datetime import datetime, timedelta

def file_to_gcs(request):
    bucket = os.environ["BUCKET"]
    today = (datetime.now() - timedelta(hours=4)).strftime('%Y-%m-%d')

    request_json = request.get_json(silent=True)
    if request_json and "dk" in request_json:
        df = pd.read_csv(request_json.get("dk"))
        df.to_csv("/tmp/dk.csv", index=False)
        upload_blob(bucket_name=bucket,
                    source_file_name="/tmp/dk.csv",
                    destination_blob_name="mlb_{}.csv".format(today.replace("-","")))
