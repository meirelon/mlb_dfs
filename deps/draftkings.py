import requests
import pandas as pd


def get_draftkings_players():
    r = requests.get("https://www.draftkings.com/lobby/getcontests?sport=MLB")
    contests = r.json().get("Contests")
    df = pd.concat([pd.DataFrame(x, index=[0]) for x in contests], axis=0, ignore_index=True)
    dg = str(df[df["gameType"] == "Classic"].iloc[0:1,:]["dg"].values[0])
    draftkings_mlb_link = "https://www.draftkings.com/lineup/getavailableplayerscsv?contestTypeId=28&draftGroupId={}"
    return draftkings_mlb_link.format(dg)
