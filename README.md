# NFL Data Engineering

A scheduled data collection pipeline that scrapes NFL data from multiple sources and stores it in S3 for downstream analytics and modeling.

## Overview

This project collects NFL data from various sources on a scheduled basis and stores it in S3 as parquet files. The data is partitioned by year and month for efficient querying and storage.

**Data Sources:**
- **Odds**: NFL betting odds and lines
- **Team Rankings**: Team statistics and rankings
- **Weather**: Game weather conditions (future)
- **Box Scores**: Game results and statistics (future)

## Architecture

```
┌─────────────────┐
│  Data Sources   │
│  (Web Scraping) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Data Collectors │
│   (Scheduled)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   S3 Storage    │
│ (Year/Month     │
│  Partitioned)   │
└─────────────────┘
```

## Data Structure

### Current Structure (Year/Month Partitioning)

Data is stored in S3 with Hive-style partitioning:

```
s3://djp-nfl-model/
└── data/raw/
    ├── odds/
    │   ├── year=2024/
    │   │   ├── month=11/data.parquet
    │   │   └── month=12/data.parquet
    │   └── year=2025/
    │       ├── month=01/data.parquet
    │       ├── month=02/data.parquet
    │       └── month=09/data.parquet
    └── team_rankings/
        ├── year=2024/
        │   ├── month=11/data.parquet
        │   └── month=12/data.parquet
        └── year=2025/
            ├── month=01/data.parquet
            └── month=09/data.parquet
```

**Key Features:**
- Each monthly partition contains deduplicated data from all collections in that month
- `timestamp` column tracks when data was collected
- Automatic upsert on new data collection (reads existing month, merges, deduplicates, writes back)
- Efficient filtering: Query last 12 weeks by reading only ~3-4 monthly files

## Setup

### Prerequisites

- Python 3.11+
- AWS credentials configured
- Poetry (optional) or pip

### Installation

1. Clone the repository:
```bash
git clone <repo-url>
cd nfl-data-engineering
```

2. Install dependencies:
```bash
# Using pip
pip install -r requirements.txt

# Using poetry
poetry install
```

3. Configure environment variables:
Create a `.env` file in the project root:
```bash
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION_NAME=us-east-1
AWS_BUCKET_NAME=djp-nfl-model
LOCAL_EXECUTION=true  # Set to false when running in AWS Lambda
```

## Usage

### Running Data Collectors

**Collect Odds Data:**
```bash
python src/data_collectors/odds_data_collector.py
```

**Collect Team Rankings Data:**
```bash
python src/data_collectors/team_rankings_data_collector.py
```

### Querying Data

**Example: Load last 12 weeks of odds data**
```python
import pandas as pd
from datetime import datetime, timedelta

# Calculate 12 weeks ago
cutoff = datetime.now() - timedelta(weeks=12)

# Determine which months to read (last 3-4 months typically)
paths = [
    "s3://djp-nfl-model/data/raw/odds/year=2025/month=08/data.parquet",
    "s3://djp-nfl-model/data/raw/odds/year=2025/month=09/data.parquet",
    "s3://djp-nfl-model/data/raw/odds/year=2025/month=10/data.parquet",
]

# Read and filter
df = pd.read_parquet(paths)
df = df[df['timestamp'] >= cutoff]

print(f"Loaded {len(df)} records from last 12 weeks")
```

**Example: Using S3Client helper**
```python
from src.s3_io.s3_client import S3Client

s3c = S3Client()

# Read specific month
df = s3c.read_dataframe_from_s3(
    bucket_name="djp-nfl-model",
    s3_key="data/raw/odds/year=2025/month=09/data.parquet"
)

# Read specific columns only (efficient!)
df = s3c.read_dataframe_from_s3(
    bucket_name="djp-nfl-model",
    s3_key="data/raw/team_rankings/year=2025/month=09/data.parquet",
    columns=['team', 'rankings_predictive_rating', 'timestamp']
)
```

## Data Migration

If you have existing daily-partitioned data, use the migration script to consolidate it into monthly partitions:

```bash
python src/migrate_to_monthly_partitions.py
```

This script:
- Reads all daily files from old structure (`YYYY/MM/DD/*.parquet`)
- Consolidates by month
- Deduplicates records (keeps latest by timestamp)
- Writes to new structure (`year=YYYY/month=MM/data.parquet`)
- Normalizes data types to prevent mixed-type errors

## Scheduled Execution

The collectors run on a schedule via GitHub Actions:

- **Odds**: Multiple times per day (before games)
- **Team Rankings**: Weekly

See `.github/workflows/` for schedule configuration.

## Data Schema

### Odds Data
- Various betting lines and odds from multiple sportsbooks
- `timestamp`: When the data was collected

### Team Rankings Data
- 1500+ statistical columns per team
- Rankings, ratings, performance metrics
- `timestamp`: When the data was collected

## Development

### Code Style

The project uses:
- `black` for formatting
- `isort` for import sorting
- `flake8` for linting
- `pre-commit` hooks for automated checks

Install pre-commit hooks:
```bash
pre-commit install
```

### Project Structure

```
nfl-data-engineering/
├── src/
│   ├── data_clients/          # Web scraping clients
│   │   ├── odds/
│   │   ├── team_rankings/
│   │   ├── weather/
│   │   └── box_scores/
│   ├── data_collectors/       # Main collection orchestrators
│   ├── s3_io/                 # S3 helper utilities
│   └── migrate_to_monthly_partitions.py
├── test/
├── config/
├── events/
├── .github/workflows/         # Scheduled jobs
└── README.md
```

## Downstream Usage

This data pipeline feeds into the **nfl-model** project for:
- Predictive modeling
- Statistical analysis
- Feature engineering

The monthly partitioning enables efficient data loading for model training and inference.

## License

Private project - All rights reserved
