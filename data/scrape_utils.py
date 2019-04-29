import requests
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

import pybaseball
from bs4 import BeautifulSoup as bs

def generate_date_range(s, e=None):
    if e is None:
        e = datetime.now().strftime("%Y-%m-%d")
    start = datetime.strptime(s, "%Y-%m-%d")
    end = datetime.strptime(e, "%Y-%m-%d")
    return [str(start + timedelta(days=x))[0:10] for x in range(0, (end-start).days)]

def get_player_info(season=2019):
    player_info = pd.read_csv("https://raw.githubusercontent.com/chadwickbureau/baseballdatabank/master/core/People.csv")
    return player_info.loc[pd.to_datetime(player_info['finalGame']).map(lambda x: str(x.year)) == str(season),:]

def batting_game_logs(playerid, season):
    try:
        url = "https://www.baseball-reference.com/players/gl.fcgi?id={playerid}&t=b&year={season}"
        r = requests.get(url.format(playerid=playerid, season=season))
        all_tags = bs(r.content, "html.parser")

        tbl = all_tags.find("table", attrs={"id":"batting_gamelogs"})
        tbl_rows = tbl.find_all('tr')
        line = []
        for tr in tbl_rows:
            td = tr.find_all('td')
            row = [tr.text for tr in td]
            line.append(row)
        df = pd.DataFrame(line)
        df.columns = ['Gcar','Gtm', 'Date','Tm','away','Opp','Rslt','Inngs','PA','AB','R','H','_2B','_3B','HR','RBI','BB','IBB','SO','HBP','SH','SF','ROE','GDP','SB','CS','BA','OBP','SLG','OPS','BOP','aLI','WPA','RE24','DK','FD', 'Pos']
        df = df.dropna().iloc[0:len(df)-2,:]
        df["bbrefID"] = [playerid for x in range(0,len(df))]
        return df
    except:
        return None

def pitching_game_logs(playerid, season):
    try:
        url = "https://www.baseball-reference.com/players/gl.fcgi?id={playerid}&t=p&year={season}"
        r = requests.get(url.format(playerid=playerid, season=season))
        all_tags = bs(r.content, "html.parser")

        tbl = all_tags.find("table", attrs={"id":"pitching_gamelogs"})
        tbl_rows = tbl.find_all('tr')
        line = []
        for tr in tbl_rows:
            td = tr.find_all('td')
            row = [tr.text for tr in td]
            line.append(row)
        df = pd.DataFrame(line)
        df.columns = ['Gcar','Gtm','Date','Tm','away','Opp','Rslt','Inngs','Dec','DR','IP','H','R','ER','BB','SO','HR','HBP','ERA','BF','Pit','Str','StL','StS','GB','FB','LD','PU','Unk','GSc','IR','IS','SB','CS','PO','AB','_2B','_3B','IBB','GDP','SF','ROE','aLI','WPA','RE24','DK','FD', 'Entered','Exited']
        df = df.dropna().iloc[0:len(df)-2,:]
        df["bbrefID"] = [playerid for x in range(0,len(df))]
        return df
    except:
        return None
