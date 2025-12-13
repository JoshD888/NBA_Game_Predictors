import pandas as pd
import glob
import numpy as np
import os

# --- Load and combine all team-season files ---
all_files = glob.glob("live_demo_games.parquet")
games_df = pd.concat([pd.read_parquet(f) for f in all_files], ignore_index=True)

# --- Clean and sort chronologically ---
games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'], format='%b %d, %Y')
games_df.sort_values(['Team_ID', 'GAME_DATE'], inplace=True)

# --- Create target variable (Win/Loss as 1/0) ---
games_df['TARGET_WL'] = games_df['WL'].map({'W': 1, 'L': 0})

# --- Add home/away feature ---
games_df['HOME'] = games_df['MATCHUP'].str.contains('vs').astype(int)

# --- Extract opponent short name ---
games_df['OPP_TEAM'] = games_df['MATCHUP'].str.extract(r'(?:vs\.|@)\s+([A-Z]+)')

# ==========================================
# SCHEDULE & FATIGUE FEATURES

def add_schedule_features(df):
    """
    Add rest days, back-to-back indicators, and fatigue metrics.
    Must be called AFTER sorting by Team_ID and GAME_DATE.
    """
    # Calculate days since last game for each team
    df['DAYS_REST'] = (
        df.groupby('Team_ID')['GAME_DATE']
        .diff()
        .dt.days
        .fillna(7)  # First game of season = assume 7 days rest
    )
    
    # Back-to-back game indicator (1 day or less)
    df['BACK_TO_BACK'] = (df['DAYS_REST'] <= 1).astype(int)
    
    # Count games in last 7 days - use manual rolling count
    def count_games_in_window(group, days):
        """Count games in the last N days for each game"""
        result = []
        dates = group['GAME_DATE'].values
        
        for i, current_date in enumerate(dates):
            # Count games in the last N days (excluding current game)
            if i == 0:
                result.append(0)
            else:
                days_diff = (current_date - dates[:i]) / np.timedelta64(1, 'D')
                count = np.sum(days_diff <= days)
                result.append(count)
        
        return pd.Series(result, index=group.index)
    
    df['GAMES_IN_LAST_7'] = (
        df.groupby('Team_ID', group_keys=False)
        .apply(lambda x: count_games_in_window(x, 7))
    )
    
    df['GAMES_IN_LAST_14'] = (
        df.groupby('Team_ID', group_keys=False)
        .apply(lambda x: count_games_in_window(x, 14))
    )
    
    # Home/Away streak (consecutive home or road games)
    df['HOME_AWAY_CHANGE'] = df.groupby('Team_ID')['HOME'].diff().fillna(0) != 0
    df['HOME_AWAY_STREAK'] = (
        df.groupby('Team_ID')['HOME_AWAY_CHANGE']
        .transform(lambda x: (~x).cumsum())
    )
    df['HOME_AWAY_STREAK'] = (
        df.groupby(['Team_ID', 'HOME_AWAY_STREAK']).cumcount() + 1
    )
    df.drop('HOME_AWAY_CHANGE', axis=1, inplace=True)
    
    # Days since last home game (travel fatigue)
    def days_since_home_game(group):
        """Calculate days since last home game for each game"""
        result = []
        last_home_date = None
        
        for idx, row in group.iterrows():
            if row['HOME'] == 1:
                result.append(0)
                last_home_date = row['GAME_DATE']
            else:
                if last_home_date is not None:
                    days_since = (row['GAME_DATE'] - last_home_date).days
                    result.append(days_since)
                else:
                    result.append(0)  # No previous home game
        
        return pd.Series(result, index=group.index)
    
    df['DAYS_SINCE_HOME'] = (
        df.groupby('Team_ID', group_keys=False)
        .apply(days_since_home_game)
    )
    
    # Season phase (early/mid/late)
    df['SEASON_YEAR'] = df['GAME_DATE'].dt.year
    # Adjust for NBA season spanning calendar years (Oct-Apr)
    df.loc[df['GAME_DATE'].dt.month <= 6, 'SEASON_YEAR'] -= 1
    
    df['SEASON_GAME_NUM'] = df.groupby(['Team_ID', 'SEASON_YEAR']).cumcount() + 1
    
    df['SEASON_PHASE'] = pd.cut(
        df['SEASON_GAME_NUM'],
        bins=[0, 20, 65, 100],
        labels=['EARLY', 'MID', 'LATE'],
        include_lowest=True
    )
    
    # One-hot encode season phase
    season_dummies = pd.get_dummies(df['SEASON_PHASE'], prefix='SEASON')
    df = pd.concat([df, season_dummies], axis=1)
    df.drop(['SEASON_PHASE', 'SEASON_GAME_NUM', 'SEASON_YEAR'], axis=1, inplace=True)
    
    return df

# Apply schedule features
print("Adding schedule and fatigue features...")
games_df = add_schedule_features(games_df)
print("Added schedule and fatigue features")

# --- 7️⃣ Compute rolling win count over last 10 games ---
def rolling_win_count(series, window=10):
    return series.shift().rolling(window, min_periods=1).sum()

games_df['TEAM_LAST10_WINS'] = (
    games_df.groupby('Team_ID')['TARGET_WL']
    .transform(lambda x: rolling_win_count(x, window=10))
)

# ==========================================
# STRENGTH OF SCHEDULE FEATURES

# Calculate team's recent win percentage (last 10 games)
games_df['TEAM_WIN_PCT_LAST10'] = games_df['TEAM_LAST10_WINS'] / 10.0

# --- 9️⃣ Compute rolling averages over last 5 games ---
stats_cols = [
    'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
    'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB',
    'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'
]

print(" Computing rolling averages...")
for col in stats_cols:
    games_df[f'{col}_rolling5'] = (
        games_df.groupby('Team_ID')[col]
        .transform(lambda x: x.shift().rolling(5, min_periods=1).mean())
    )

# ==========================================
# ADVANCED EFFICIENCY METRICS

def add_efficiency_metrics(df):
    """
    Add Four Factors and advanced efficiency metrics.
    """
    # Effective Field Goal % (accounts for 3-pointers being worth more)
    df['EFG_PCT_rolling5'] = (
        (df['FGM_rolling5'] + 0.5 * df['FG3M_rolling5']) / 
        df['FGA_rolling5']
    ).fillna(0)
    
    # Turnover Rate (turnovers per 100 possessions)
    possessions = df['FGA_rolling5'] + 0.44 * df['FTA_rolling5'] + df['TOV_rolling5']
    df['TOV_RATE_rolling5'] = (df['TOV_rolling5'] / possessions * 100).fillna(0)
    
    # Offensive Rebounding Rate
    df['OREB_RATE_rolling5'] = (
        df['OREB_rolling5'] / df['FGA_rolling5']
    ).fillna(0)
    
    # Free Throw Rate (free throws attempted per field goal attempt)
    df['FT_RATE_rolling5'] = (
        df['FTA_rolling5'] / df['FGA_rolling5']
    ).fillna(0)
    
    # Pace (possessions per game) - approximate
    df['PACE_rolling5'] = possessions / 5.0  # Average over 5 games
    
    return df

print("Adding efficiency metrics...")
games_df = add_efficiency_metrics(games_df)
print("Added efficiency metrics")

# --- Prepare opponent rolling stats & features ---
rolling_cols = [c for c in games_df.columns if c.endswith('_rolling5')]
schedule_cols = [
    'DAYS_REST', 'BACK_TO_BACK', 'GAMES_IN_LAST_7', 'GAMES_IN_LAST_14',
    'HOME_AWAY_STREAK', 'DAYS_SINCE_HOME'
]
season_cols = [c for c in games_df.columns if c.startswith('SEASON_')]
opp_features = ['TEAM_LAST10_WINS', 'TEAM_WIN_PCT_LAST10'] + rolling_cols + schedule_cols + season_cols

# Helper table with Game_ID, Team_ID, and all features
opp_rolls = games_df[['Game_ID', 'Team_ID'] + opp_features].copy()

# Merge to attach opponent features
print("Merging opponent features...")
merged = games_df.merge(
    opp_rolls,
    how='left',
    on='Game_ID',
    suffixes=('', '_opp')
)

# Keep only true team–opponent pairs
merged = merged[merged['Team_ID'] != merged['Team_ID_opp']]

# ==========================================
# CREATE DIFFERENTIAL FEATURES

print("Creating differential features...")

# Win percentage differential
merged['WIN_PCT_DIFF'] = (
    merged['TEAM_WIN_PCT_LAST10'] - merged['TEAM_WIN_PCT_LAST10_opp']
)

# Key stat differentials
merged['FG_PCT_DIFF'] = (
    merged['FG_PCT_rolling5'] - merged['FG_PCT_rolling5_opp']
)

merged['REB_MARGIN'] = (
    merged['REB_rolling5'] - merged['REB_rolling5_opp']
)

merged['TOV_DIFF'] = (
    merged['TOV_rolling5'] - merged['TOV_rolling5_opp']
)

merged['PACE_MATCHUP'] = (
    merged['PACE_rolling5'] * merged['PACE_rolling5_opp']
)

# Rest advantage (your rest days - opponent's rest days)
merged['REST_ADVANTAGE'] = (
    merged['DAYS_REST'] - merged['DAYS_REST_opp']
)

print("Added differential features")

# --- Drop unnecessary columns ---
drop_cols = [
    'W', 'L', 'WL', 'W_PCT', 'MATCHUP', 'MIN',
    'FGM_rolling5', 'FG3M_rolling5', 'FTM_rolling5', 'REB_rolling5',
    'FGM_rolling5_opp', 'FG3M_rolling5_opp', 'FTM_rolling5_opp', 'REB_rolling5_opp',
    'OPP_TEAM', 'GAME_DATE'
]

# Also drop raw stat columns
drop_cols += stats_cols

merged = merged.drop(columns=[c for c in drop_cols if c in merged.columns])

# --- Drop NA rows ---
initial_rows = len(merged)
merged = merged.dropna()
print(f"Dropped {initial_rows - len(merged)} rows with NA values. Remaining rows: {len(merged)}")

# --- 14.5️⃣ Check correlations and remove redundant features ---
print("Analyzing feature correlations...")

# Only analyze numeric columns (exclude Team_ID, Game_ID, etc.)
numeric_cols = merged.select_dtypes(include=[np.number]).columns
corr = merged[numeric_cols].corr()
upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))

# Show pairs with correlation > 0.85 (highly correlated)
high_corr = [(col, row, upper.loc[row, col]) 
             for col in upper.columns 
             for row in upper.index 
             if pd.notnull(upper.loc[row, col]) and abs(upper.loc[row, col]) > 0.85]

if high_corr:
    print(f"Found {len(high_corr)} highly correlated feature pairs (|r| > 0.85)")
    # Optionally print top 5 to see what's correlated
    # for pair in high_corr[:5]:
    #     print(f"  {pair[1]} <-> {pair[0]}: {pair[2]:.3f}")

print("Removing redundant features...")

# List of redundant features to drop
redundant_cols = [
    # 1. Win percentage is just wins/10 - keep TEAM_LAST10_WINS (more interpretable)
    'TEAM_WIN_PCT_LAST10',
    'TEAM_WIN_PCT_LAST10_opp',
    
    # 2. Rate metrics are linear transformations of raw stats
    # Keep the rate versions (they're normalized and more comparable)
    'OREB_rolling5',  # Keep OREB_RATE_rolling5 instead
    'OREB_rolling5_opp',
    
    'TOV_rolling5',  # Keep TOV_RATE_rolling5 instead
    'TOV_rolling5_opp',
    
    'FTA_rolling5',  # Keep FT_RATE_rolling5 instead
    'FTA_rolling5_opp',
    
    # 3. EFG_PCT is better than FG_PCT (accounts for 3-pointers)
    # Keep EFG_PCT_rolling5, drop FG_PCT_rolling5
    'FG_PCT_rolling5',
    'FG_PCT_rolling5_opp',
    
    # 4. Symmetric game-context features (both teams share same game properties)
    'DAYS_REST_opp',  # Keep DAYS_REST and REST_ADVANTAGE
    'SEASON_EARLY_opp',  # Both teams in same season
    'SEASON_MID_opp',
    'SEASON_LATE_opp',
]

# Drop columns that exist in the dataframe
cols_to_drop = [col for col in redundant_cols if col in merged.columns]
merged = merged.drop(columns=cols_to_drop)

# Update WIN_PCT_DIFF calculation since we dropped WIN_PCT features
# Recalculate it properly from TEAM_LAST10_WINS
if 'WIN_PCT_DIFF' in merged.columns:
    merged['WIN_PCT_DIFF'] = (
        merged['TEAM_LAST10_WINS'] - merged['TEAM_LAST10_WINS_opp']
    ) / 10.0  # Normalize to -1 to 1 scale

print(f"Dropped {len(cols_to_drop)} redundant features")

# --- Save processed dataset ---
output_path = "demo.parquet"
merged.to_parquet(output_path, index=False)
print(f"Saved processed dataset: {output_path}")

# Print summary
print("\n" + "="*60)
print("FEATURE SUMMARY")
print("="*60)
print(f"Schedule Features: {schedule_cols}")
print(f"Efficiency Features: {['EFG_PCT_rolling5', 'TOV_RATE_rolling5', 'OREB_RATE_rolling5', 'FT_RATE_rolling5', 'PACE_rolling5']}")
print(f"Differential Features: {['WIN_PCT_DIFF', 'FG_PCT_DIFF', 'REB_MARGIN', 'TOV_DIFF', 'PACE_MATCHUP', 'REST_ADVANTAGE']}")
print(f"\nTotal features: {len(merged.columns)}")
print(f"Total observations: {len(merged)}")
print("="*60)
