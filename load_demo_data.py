import pandas as pd
from nba_api.stats.endpoints import TeamGameLog
from nba_api.stats.static import teams
import time

recent_games = []

for team in teams.get_teams():
    try:
        tg = TeamGameLog(team_id=team['id'], season="2025-26", season_type_all_star="Regular Season")
        df = tg.get_data_frames()[0].head(15)  # last 10 games
        df['Team_ID'] = team['id']
        recent_games.append(df)
    except Exception as e:
        print(team['full_name'], e)
    time.sleep(1.2)  # respect API rate limit

live_demo_df = pd.concat(recent_games, ignore_index=True)
live_demo_df.to_parquet("live_demo_games.parquet", engine="pyarrow", index=False)

print(live_demo_df.head(n=10))