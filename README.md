# NBA Game Outcome Prediction

## Project Overview
This project predicts NBA game outcomes using supervised machine learning models trained on team-level performance metrics and contextual features. Multiple algorithms—including Logistic Regression, SVM, Random Forest, and XGBoost—are evaluated under a unified pipeline to assess predictive performance and fundamental accuracy limits.

## Repository Structure

code/
├── load_game_stats.py # Collects raw team game logs
├── prepare_features.py # Builds final modeling dataset
├── models/
│ ├── baseline_models.py
│ ├── xgboost_model.py
│ └── ensemble_models.py
├── evaluation.py
├── visualization.py
data/
results/
notebooks/


## Data Pipeline Overview
The data pipeline consists of two stages:

1. **Raw data collection (Optional)**: Team-level game logs are collected from NBA.com using the `nba_api` client and saved as individual Parquet files by team and season.
2. **Feature construction**: All team-season files are combined and transformed into a single modeling dataset containing rolling statistics, schedule/fatigue metrics, opponent features, and differential features.

This separation allows experiments to be reproduced without repeated API calls while preserving the ability to fully regenerate the dataset in the future.

## Using the Preprocessed Dataset (Quick Start)
All models in this repository are trained using a fully processed dataset saved as:

all_teams_last10seasons_with_opponent_rolls.parquet

No data scraping is required. Ensure this file is present in the project root or `data/` directory, then proceed directly to model training.

## Full Pipeline: Reproducing the Dataset from Scratch

### Step 1: Collect Team Game Logs (Optional)
To collect raw team-level game logs:

python load_game_stats.py

This script:
- Queries NBA.com via the `nba_api` client
- Collects regular-season games for all teams from 2021–2022 through 2025–2026
- Saves one Parquet file per team per season to `team_game_data/`
- Throttles requests to one call every 1.2 seconds to avoid API rate limiting

### Step 2: Build Features and Final Dataset
Once team-season files are available:

python prepare_features.py

This script:
- Loads and combines all team-season Parquet files
- Sorts games chronologically by team
- Constructs the target variable (win/loss)
- Adds home/away indicators and opponent identifiers
- Computes schedule and fatigue features (rest days, back-to-backs, recent game density)
- Generates rolling performance metrics (5-game averages)
- Derives advanced efficiency metrics (eFG%, turnover rate, rebounding rate, pace)
- Merges opponent rolling statistics
- Creates matchup-level differential features
- Removes redundant and highly correlated features
- Outputs a single cleaned dataset: all_teams_last10seasons_with_opponent_rolls.parquet

## Reproducibility Notes
- The modeling dataset is generated deterministically given the same raw inputs.
- Minor differences may arise if NBA.com data is updated or extended with new seasons.
- Chronological train-test splits are used to prevent data leakage.
