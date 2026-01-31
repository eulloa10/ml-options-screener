# ML-Powered Covered Call Screener (Active Development)

An end-to-end data engineering and machine learning pipeline designed to identify, analyze, and predict high-probability covered call opportunities.

It fetches data from **Yahoo Finance**, calculates metrics (Greeks, Risk/Reward, Annualized Return), and filters based on risk management criteria.

The results are automatically processed and uploaded to an **AWS S3** Data Lake as Parquet files for historical analysis and backtesting.

## Features

* **Multi-Source Data:** Integrates Yahoo Finance (market data) and FRED (Risk-Free Rate).
* **Analytics:** Calculates Black-Scholes Greeks (Delta, Gamma, Theta, Vega) and proprietary risk metrics.
* **Filtering:** Filters options based on Volume, Open Interest, Delta, Return on Risk and other metrics.
* **Win/Loss Classification:** Automatically fetches historical closing prices to label past trades as "Profitable" or "Loss" for ML training.
* **Data Archiving:** Automatically moves expired raw data to an archive folder to optimize performance and storage.
* **Cloud Native:** Runs on **GitHub Actions** with a dual-schedule:
    * **Daily:** Scans the market for new trades (M-F).
    * **Weekly:** Labels expired trades and updates the training set (Mondays).

---

## Project Roadmap

**Machine Learning Integration (In Progress)**
* [x] Build "Ground Truth" pipeline to label historical data
* [ ] Exploratory Data Analysis (EDA) on collected S3 data
* [ ] Feature Engineering (e.g., Vol/OI Ratio, Distance to Strike)
* [ ] Train classification model to predict probability of profit
* [ ] Implement model inference in the daily pipeline


## Setup & Installation

### Prerequisites
* Python 3.13+
* [`uv`](https://github.com/astral-sh/uv)
* [AWS Account](https://aws.amazon.com/s3/) (S3 bucket)
* [FRED API Key](https://fred.stlouisfed.org/docs/api/api_key.html)
* [GCP Account for Google Sheets API](https://developers.google.com/workspace/sheets/api/guides/concepts)

### Installing Dependencies
This project uses [`uv`](https://docs.astral.sh/uv/) for dependency management
```bash
uv sync --frozen
```
### Configuration

This project uses a **private** configuration file to store the watchlist and screening parameters.

1. Copy the example file to create your local config:

   ```bash
   cp config.example.py config.py
   ```
2. Edit `config.py` with your own stocks and risk parameters (e.g., MIN_VOLUME, MIN_DELTA).

### Environment Variables

1. Create a .env file in the root directory and populate your keys.

   ```bash
   cp .env.example .env
   ```

### Usage
#### Run the Daily Screener
Scans the market for current opportunities and uploads to S3 and Google Sheets.

   ```bash
   uv run screener/main.py
   ```
#### Run the Labeling Pipeline (ML Data)
Scans historical data in S3. If a trade has expired, it fetches the actual closing price to determine if it was a "Win" or "Loss" and saves it to training data bucket

   ```bash
   uv run screener/scripts/generate_labels.py
   ```
---

# GitHub Actions Deployment

The project runs on two separate schedules:

| Workflow | Schedule | Description |
| :--- | :--- | :--- |
| **Daily Screener** | Mon-Fri @ 10:00 AM PT | Fetches live data, screens options, uploads to S3 & Google Sheets. |
| **Weekly Labeling** | Mondays @ 6:30 AM PT | Checks past trades, labels outcomes, updates ML datasets. |

- Manual: You can manually trigger a run via the Actions tab > Daily Option Screener > Run workflow.

## Required GitHub Secrets

Navigate to:

**Settings → Secrets and variables → Actions**

and add the following secrets:

| Secret Name              | Description                                                                 |
|--------------------------|-----------------------------------------------------------------------------|
| `AWS_ACCESS_KEY_ID`      | IAM User Access Key                  |
| `AWS_SECRET_ACCESS_KEY`  | IAM User Secret Key.                                                        |
| `S3_BUCKET_NAME`         | The name of your S3 bucket (e.g., `my-data-lake`).                          |
| `FRED_API_KEY`           | API key from Federal Reserve Economic Data.                                 |
| `CONFIG_FILE_BASE64`     | Your `config.py` file encoded as a Base64 string.              |
| `GOOGLE_SHEET_ID`        | The long ID string found in your Google Sheet URL.              |
| `GOOGLE_CREDENTIALS_BASE64`     | Your Service Account JSON key encoded as a Base64 string.              |

The `config.py` file itself is **injected into the runner during the build process**, rather than being stored in the repository.

### How to Generate `CONFIG_FILE_BASE64`

Since `config.py` is **not stored in the repository**, you must encode your local version into a Base64 string that GitHub can read and inject at runtime.

#### Mac / Linux

Run the following command in your terminal:

```bash
base64 -i config.py | pbcopy
``` 
This encodes `config.py` and copies the Base64 output directly to your clipboard.

#### Windows (PowerShell)
Run the following command in PowerShell:

```powershell
[Convert]::ToBase64String([IO.File]::ReadAllBytes(".\config.py")) | Set-Clipboard
``` 
This reads `config.py`, encodes it as Base64, and copies the result to your clipboard.
