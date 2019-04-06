import argparse
import scrape_utils

def run(season):
    player_info = get_player_info()

    batting = [batting_game_logs(pid, season=season) for pid in player_info['bbrefID']]
    batting_df = pd.concat([b for b in batting if b is not None], axis=0)

    pitching = [pitching_game_logs(pid, season=season) for pid in player_info['bbrefID']]
    pitching_df = pd.concat([p for p in pitching if p is not None], axis=0)

    return [batting_df, pitching_df]

def main(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--season',
                        dest='season',
                        default = None,
                        help='MLB Season')
    parser.add_argument('--project_id',
                        dest='project_id',
                        default = None,
                        help='project')

    args, _ = parser.parse_known_args(argv)

    b,p = run(args.season)

    b.to_gbq(project_id=args.project_id,
                                 destination_table="baseball.batting_{}".format(season),
                                 if_exists="replace",
                                 chunksize=1000)
    p.to_gbq(project_id=args.project_id,
                                 destination_table="baseball.pitching_{}".format(season),
                                 if_exists="replace",
                                 chunksize=1000)
if __name__ == '__main__':
    main()
