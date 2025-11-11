from nba_api.stats.static import teams
import pandas as pd

# Get list of all teams
nba_teams = teams.get_teams()

# Convert to DataFrame
teams_df = pd.DataFrame(nba_teams)

# Optionally select only the columns you want
teams_df = teams_df[['id', 'abbreviation']]

# Rename id column to Team_ID for clarity
teams_df = teams_df.rename(columns={'id': 'Team_ID'})

# Save to CSV or inspect
teams_df.to_csv('tables/nba_team_lookup.csv', index=False)
print(teams_df.head())
