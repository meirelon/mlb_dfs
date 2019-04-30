import requests
import pandas as pd

def get_draftkings_players():
    r = requests.get("https://www.draftkings.com/lobby/getcontests?sport=MLB")
    contests = r.json().get("Contests")
    dg = [x.get("dg") for x in contests if x.get("gameType") == "Classic"]
    url = "https://www.draftkings.com/lineup/getavailableplayerscsv?contestTypeId=28&draftGroupId={}"
    i=0
    while True:
        players = pd.read_csv(url.format(dg[i]))
        i+=1
        if players.shape[0] < 1:
            continue
        else:
            break
    return players
