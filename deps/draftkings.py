import requests
from functools import reduce
import gcsfs

import pandas as pd

def get_draftkings_players():
    col_heads = ["position", "name_and_id", "name", "id", "roster_position", "salary", "game_info", "teamabbrev", "avgpointspergame"]
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
    players.columns = col_heads
    return players

def get_draftkings_predictions(project, dataset_base, dataset_dfs, dt, min_salary=3500):

    q = """with
    dk as(
    select *
    from `{project}.{dataset_dfs}.mlb_draftkings_*`
    where _table_suffix = (select max(_table_suffix) from `{project}.{dataset_dfs}.mlb_draftkings_*`)
    and avgpointspergame > 0 and salary > {min_salary}
    ),

    injuries as(
    select distinct concat(name_first, " ", name_last) as name
    from `{project}.{dataset_base}.injuries_*`
    where _table_suffix = (select max(_table_suffix) from `{project}.{dataset_base}.injuries_*`)
    ),

    probable_starters as(
    select position, name_and_id, name,	id,	roster_position, salary, game_info, teamabbrev,	avgpointspergame
    from dk
    join(
    select concat(first_name, " ", last_name) as name, tm as teamabbrev
    from `{project}.{dataset_base}.probable_pitchers_*`
    where _table_suffix = (select max(_table_suffix) from `{project}.{dataset_base}.probable_pitchers_*`)
    group by 1,2
    )
    using (name, teamabbrev)
    ),

    predictions as(
    select name, teamabbrev, prediction
    from `{project}.{dataset_dfs}.mlb_dk_predictions_{dt}` a
    join `{project}.{dataset_base}.mlbam_team_mapping` b
    on a.tm = b.mlbam_team
    ),

    raw as(
    select *
    from dk
    where name not in (select * from injuries) and position != "SP"
    union all(
    select *
    from probable_starters
    )
    )

    select position, name_and_id, name,	id,	roster_position, salary, game_info, teamabbrev,	prediction
    from(
    select * except(avgpointspergame, prediction),
    case when prediction is null then avgpointspergame else prediction end as prediction
    from raw
    left join predictions
    using(name, teamabbrev)
    )
    """

    query_formatted = q.format(project=project,
                               dataset_base=dataset_base,
                               dataset_dfs=dataset_dfs,
                               dt=dt,
                               min_salary=min_salary)

    df = pd.read_gbq(project_id=project, query=query_formatted, dialect="standard")
    df.columns = ["Position","Name + ID","Name","ID","Roster Position","Salary","Game Info","TeamAbbrev","AvgPointsPerGame",]
    return df


class dkLineupExport:
    def __init__(self, project, dataset, bucket, dt):
        self.project = project
        self.dataset = dataset
        self.bucket = bucket
        self.dt = dt

    def get_dk_players(self):
        q = "select * from `{project}.{dataset}.mlb_draftkings_{dt}`"
        query_formatted = q.format(project=self.project, dataset=self.dataset, dt=self.dt)
        df = pd.read_gbq(project_id=self.project,
                         query=query_formatted,
                         dialect="standard")
        return df

    def get_dk_lineups(self):
        lineups = pd.read_csv("gs://{bucket}/lineups/daily_dk_lineups.csv".format(bucket=self.bucket))
        return lineups

    def get_lineups_with_id(self):
        print("first")
        df = self.get_dk_players()
        print("second")
        lineups = self.get_dk_lineups()

        lineups["name"] = lineups["first"] + " " + lineups["last"]
        lineups_with_id = lineups.set_index("name").join(df[["name", "name_and_id"]].set_index("name")).reset_index()
        return lineups_with_id

    def lineup_format(self, df):
        position_cols = ["P", "P",	"C", "1B", "2B", "3B", "SS", "OF", "OF", "OF"]
        position_order = ["P", "C", "1B", "2B", "3B", "SS", "OF"]
        position_values = [list(df[df["pos"].isin([x])]["name_and_id"].values) for x in position_order]
        position_values_flatmap = reduce(list.__add__, position_values)
        dfs_df = pd.DataFrame(position_values_flatmap).transpose()
        dfs_df.columns = position_cols
        return dfs_df


    def run(self, total_lineups=None):
        lineups_with_id = self.get_lineups_with_id()

        df_split = [x for _, x in lineups_with_id.groupby("lineup_number")]
        df_list = [self.lineup_format(d) for d in df_split]
        return pd.concat(df_list, axis=0, ignore_index=True)
