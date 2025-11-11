import pandas as pd
import glob
import numpy as np
import os

# --- 1️⃣ Load and combine all team-season files ---
all_files = glob.glob("team_game_data/*.parquet")
games_df = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)

# --- 2️⃣ Clean and sort chronologically ---
games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'], format='%b %d, %Y')
games_df.sort_values(['Team_ID', 'GAME_DATE'], inplace=True)

# --- 3️⃣ Create target variable (Win/Loss as 1/0) ---
games_df['TARGET_WL'] = games_df['WL'].map({'W': 1, 'L': 0})

# --- 4️⃣ Add home/away feature ---
games_df['HOME'] = games_df['MATCHUP'].str.contains('vs').astype(int)  # 1 if home, 0 if away

# --- 5️⃣ Extract opponent short name (e.g., "BOS", "NYK") ---
games_df['OPP_TEAM'] = games_df['MATCHUP'].str.extract(r'(?:vs\.|@)\s+([A-Z]+)')

# --- 6️⃣ Compute rolling win count over last 10 games (no leakage) ---
def rolling_win_count(series, window=10):
    return series.shift().rolling(window, min_periods=1).sum()

games_df['TEAM_LAST10_WINS'] = (
    games_df.groupby('Team_ID')['TARGET_WL']
    .transform(lambda x: rolling_win_count(x, window=10))
)

# --- 7️⃣ Compute rolling averages over last 5 games (shift ensures no leakage) ---


stats_cols = [
    'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
    'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB',
    'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'
]

for col in stats_cols:
    games_df[f'{col}_rolling5'] = (
        games_df.groupby('Team_ID')[col]
        .transform(lambda x: x.shift().rolling(5, min_periods=1).mean())
    )

# --- 8️⃣ Prepare opponent rolling stats & last-10 wins ---
rolling_cols = [c for c in games_df.columns if c.endswith('_rolling5')]
opp_features = ['TEAM_LAST10_WINS'] + rolling_cols

# Helper table with Game_ID, Team_ID, and rolling features
opp_rolls = games_df[['Game_ID', 'Team_ID'] + opp_features].copy()

# Merge to attach opponent features
merged = games_df.merge(
    opp_rolls,
    how='left',
    on='Game_ID',
    suffixes=('', '_opp')
)

# Keep only true team–opponent pairs (drop self-merge)
merged = merged[merged['Team_ID'] != merged['Team_ID_opp']]

# --- 9️⃣ Drop unnecessary or redundant columns ---
drop_cols = [
    'W', 'L', 'WL', 'W_PCT', 'MATCHUP', 'MIN',
    'FGM_rolling5', 'FG3M_rolling5', 'FTM_rolling5', 'REB_rolling5',
    'FGM_rolling5_opp', 'FG3M_rolling5_opp', 'FTM_rolling5_opp', 'REB_rolling5_opp', 'OPP_TEAM'
]

# Also drop the raw stat columns
drop_cols += stats_cols

merged = merged.drop(columns=[c for c in drop_cols if c in merged.columns])

# --- 🔟 Drop NA rows (first few games, missing merges, etc.) ---
merged = merged.dropna()
print(f"✅ Dropped rows with NA values. Remaining rows: {len(merged)}")

# --- 11️⃣ Save processed dataset ---
output_path = "all_teams_last10seasons_with_opponent_rolls.parquet"
merged.to_parquet(output_path, index=False)
print(f"✅ Saved processed dataset: {output_path}")
