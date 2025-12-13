# NBA Game Outcome Prediction
## Project Overview

This project predicts NBA game outcomes using supervised machine learning models trained on team-level performance metrics and contextual features. Multiple algorithms—including Logistic Regression, SVM, Random Forest, and XGBoost—are evaluated under a unified pipeline to assess predictive performance and fundamental accuracy limits.

## Data Pipeline Overview

The data pipeline consists of two stages:

Raw data collection (Optional): Team-level game logs are collected from NBA.com using the nba_api client and saved as individual Parquet files by team and season.

Feature construction: All team-season files are combined and transformed into a single modeling dataset containing rolling statistics, schedule/fatigue metrics, opponent features, and differential features.

This separation allows experiments to be reproduced without repeated API calls while preserving the ability to fully regenerate the dataset in the future.

## Using the Preprocessed Dataset (Quick Start)

All models in this repository are trained using a fully processed dataset saved as:

all_teams_last10seasons_with_opponent_rolls.parquet

No data scraping is required. Ensure this file is present in the project root or data/ directory, then proceed directly to model training.

## Full Pipeline: Reproducing the Dataset from Scratch
### Step 1: Collect Team Game Logs (Optional)

Run:

python load_game_stats.py

This script:

Queries NBA.com via the nba_api client

Collects regular-season games for all teams from the 2021–2022 through the 2024-2025 season.

Saves one Parquet file per team per season to team_game_data/

Throttles requests to one call every 1.2 seconds to avoid API rate limiting

### Step 2: Build Features and Final Dataset

Run:

python prepare_features.py

This script:

Loads and combines all team-season Parquet files

Sorts games chronologically by team

Constructs the target variable (win/loss)

Adds home/away indicators and opponent identifiers

Computes schedule and fatigue features (rest days, back-to-backs, recent game density)

Generates rolling performance metrics (5-game averages)

Derives advanced efficiency metrics (eFG%, turnover rate, rebounding rate, pace)

Merges opponent rolling statistics

Creates matchup-level differential features

Removes redundant and highly correlated features

Outputs a single cleaned dataset: all_teams_last10seasons_with_opponent_rolls.parquet

## Demo Pipeline (Optional)

To run the demo with new NBA games:

Collect new game data:

python load_demo_data.py

Build demo features:

python prepare_demo_features.py

Run demo notebook:

demo.ipynb

This notebook trains models on the full historical dataset and evaluates predictions using the new demo data.

## Reproducibility Notes

The modeling dataset is generated deterministically given the same raw inputs.

Minor differences may arise if NBA.com data is updated or extended with new seasons.

Chronological train-test splits are used to prevent data leakage.

Hyperparameter tuning results are pre-saved; retraining models with tuned parameters reproduces the reported performance.
