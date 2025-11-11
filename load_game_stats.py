import pandas as pd
import time
from nba_api.stats.endpoints import TeamGameLog
from nba_api.stats.static import teams
import os

# ----------------------
# CONFIG
# ----------------------
all_teams = teams.get_teams()  # list of dicts with team info
seasons = [f"{y}-{str(y+1)[-2:]}" for y in range(2021, 2026)]  # last 10 seasons
season_type = "Regular Season"
output_dir = "team_game_data/"
rate_limit = 1.2  # seconds between API calls

# Make output directory
os.makedirs(output_dir, exist_ok=True)

# ----------------------
# LOOP OVER TEAMS AND SEASONS
# ----------------------
for team in all_teams:
    team_name = team['full_name']
    team_id = team['id']
    print(f"\n=== Processing {team_name} ===")

    for season in seasons:
        print(f"Fetching {season} ...")
        try:
            tg = TeamGameLog(
                team_id=team_id,
                season=season,
                season_type_all_star=season_type,
                league_id_nullable='00'
            )
            df = tg.get_data_frames()[0]

            if df.empty:
                print(f"⚠️ No games found for {team_name} in {season}")
                continue

            # Save to Parquet
            filename = f"{output_dir}{team_name.replace(' ','_')}_{season}_games.parquet"
            df.to_parquet(filename, engine="pyarrow", index=False)
            print(f"✅ Saved {len(df)} games to {filename}")

        except Exception as e:
            print(f"❌ Error fetching {team_name} {season}:", e)

        time.sleep(rate_limit)
