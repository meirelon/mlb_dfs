import pandas as pd

class inputData:
    def __init__(self, project, dataset, yesterday, today):
        self.project = project
        self.dataset = dataset
        self.today = today
        self.yesterday = yesterday

    def get_query(self, _yesterday,_today):
        q = """
            with
            batting as(
            select date, name, mlbam_team as tm,
            age, ((h-_2b-_3b-hr)*3) + (_2b*5) + (_3b*8) + (hr*10) + (rbi*2) + (r*2) + (bb*2) + (hbp*2) + (sb*5) as dk
            from `{project}.{dataset}.batting_2019*`
            join `{project}.{dataset}.mlbam_team_mapping`
            using(tm, lev)
            ),

            hitting_streaks as(
            select date, name, tm,
            max(onbase_streak_number) over(partition by name,tm order by date rows between 5 preceding and 1 preceding) as hitting_streak_max,
            avg(onbase_streak_number) over(partition by name,tm order by date rows between 5 preceding and 1 preceding) as hitting_streak_min
            from `{project}.{dataset}.onbase_streaks_2019`
            ),

            player_id_master as(
            select concat(FIRSTNAME, " ", LASTNAME) as name, MLBCODE
            from `{project}.{dataset}.playerid_master`
            ),


            statcast_agg as(
            select game_date, batter,
            count(1) as bb_count,
            sum(hc_x) as hc_x,
            sum(hc_y) as hc_y,
            sum(launch_speed) as launch_speed,
            sum(launch_angle) as launch_angle,
            sum(pitch_number) as pitch_number
            from `{project}.{dataset}.statcast_2019*`
            where bb_type is not null
            group by 1,2
            ),

            statcast as(
            select cast(date(game_date) as string) as date, name,
            count(1) over(partition by batter_id order by game_date rows between 15 preceding and 1 preceding) as bb_count,
            avg(hc_x) over(partition by batter_id order by game_date rows between 15 preceding and 1 preceding) as hc_x_mean,
            avg(hc_y) over(partition by batter_id order by game_date rows between 15 preceding and 1 preceding) as hc_y_mean,
            avg(launch_speed) over(partition by batter_id order by game_date rows between 15 preceding and 1 preceding) as launch_speed_mean,
            avg(launch_angle) over(partition by batter_id order by game_date rows between 15 preceding and 1 preceding) as launch_angle_mean,
            avg(pitch_number) over(partition by batter_id order by game_date rows between 15 preceding and 1 preceding) as pitch_number_mean
            from(
            select *, cast(batter as string) as batter_id
            from statcast_agg a
            join player_id_master b
            on cast(a.batter as int64) = b.MLBCODE
            )
            ),

            weather as(
            select * except(date)
            from `{project}.{dataset}.weather_{weather_date}`
            ),

            raw as(
            select * except(dk)
            from(
            select *,
            avg(dk) over(partition by name,tm order by date rows between 5 preceding and 1 preceding) as five_day_dk_avg,
            max(dk) over(partition by name,tm order by date rows between 5 preceding and 1 preceding) as five_day_dk_max,
            min(dk) over(partition by name,tm order by date rows between 5 preceding and 1 preceding) as five_day_dk_min,
            avg(dk) over(partition by name,tm order by date rows between 3 preceding and 1 preceding) as three_day_dk_avg,
            max(dk) over(partition by name,tm order by date rows between 3 preceding and 1 preceding) as three_day_dk_max,
            min(dk) over(partition by name,tm order by date rows between 3 preceding and 1 preceding) as three_day_dk_min,
            stddev(dk) over(partition by name,tm order by date rows between 10 preceding and 1 preceding) as dk_std
            from batting
            )
            join hitting_streaks
            using(date, name, tm)
            join statcast
            using(date, name)
            where dk_std is not null and date = "{batting_date}"
            )

            select *
            from raw a
            join weather b
            on a.tm = b.home or a.tm = b.away
            """
        return q.format(project=self.project, dataset=self.dataset, weather_date=_today.replace("-",""), batting_date=_yesterday)

    def get_input_data(self, days_back=4):
        df = pd.read_gbq(project_id=self.project, query=self.get_query(self.yesterday, self.today), dialect="standard")
        if df.shape[0] == 0:
            for i in range(2,days_back):
                new_yesterday = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                df = pd.read_gbq(project_id=self.project, query=self.get_query(new_yesterday, self.today), dialect="standard")
                if df.shape[0] > 0:
                    return df
                    break
                else:
                    continue
        else:
            return df

    def run(self):
        select_cols = ["name","tm", 'age', 'apparentTemperatureHigh', 'apparentTemperatureHighTime',
       'apparentTemperatureLow', 'apparentTemperatureLowTime',
       'apparentTemperatureMax', 'apparentTemperatureMaxTime',
       'apparentTemperatureMin', 'apparentTemperatureMinTime', 'cloudCover',
       'dewPoint', 'humidity', 'moonPhase',
       'precipIntensity', 'precipIntensityMax', 'precipIntensityMaxTime',
       'precipProbability', 'pressure', 'sunriseTime', 'sunsetTime',
       'temperatureHigh', 'temperatureHighTime', 'temperatureLow',
       'temperatureLowTime', 'temperatureMax', 'temperatureMaxTime',
       'temperatureMin', 'temperatureMinTime', 'time', 'uvIndex',
       'uvIndexTime', 'visibility', 'windBearing', 'windGust', 'windGustTime',
       'windSpeed', 'five_day_dk_avg', 'five_day_dk_max', 'five_day_dk_min',
       'three_day_dk_avg', 'three_day_dk_max', 'three_day_dk_min', 'dk_std',
       'hitting_streak_max', 'hitting_streak_min', 'bb_count', 'hc_x_mean',
       'hc_y_mean', 'launch_speed_mean', 'launch_angle_mean',
       'pitch_number_mean']
        input_data = self.get_input_data()
        return input_data[select_cols]
