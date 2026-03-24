# ML-Powered Covered Call Platform (Active Development)

An end-to-end data engineering and machine learning pipeline designed to identify, analyze, and predict high-probability covered call opportunities. This system moves data from raw market feeds to a live, mobile-optimized dashboard.

## Features

* **Data Lake:** Fetches live data from Yahoo Finance and FRED, storing Parquet files in AWS S3 for backtesting.
* **Machine Learning:** Uses XGBoost to calculate a Win Probability for every trade based on Greeks (Delta, Theta, Gamma), implied volatility, and historical regime markers.
* **Database:** Real-time persistence via Supabase (PostgreSQL) for sub-second dashboard performance.
* **Dashboard:** A Streamlit interface accessible via mobile. Features a "Passcode Bouncer" for security and a "Historical Archive" for trade auditing.
* **Automated Labeling:** A weekly "Backfiller" script that fetches closing prices to label expired trades as Wins/Losses, creating a self-improving feedback loop for the ML model.
* **Cloud Native:** Fully orchestrated via GitHub Actions with an event-driven trigger system.

---

## Project Roadmap

**Current Phase: Evaluation & Refinement**
* [ ] [NEXT] Implement Model Monitoring (Accuracy vs. Confidence charts)
* [ ] Implement **SendGrid** Email and SMS alerting logic
* [ ] Integrate **LangChain** Data Agent for natural language insights

## Setup & Installation

### Prerequisites
* Python 3.13+
* [`uv`](https://github.com/astral-sh/uv)
* [AWS Account](https://aws.amazon.com/s3/) (S3 bucket)
* [FRED API Key](https://fred.stlouisfed.org/docs/api/api_key.html)
* [GCP Account for Google Sheets API](https://developers.google.com/workspace/sheets/api/guides/concepts)
* [Supabase Account](https://supabase.com/)
* [Streamlit Community Cloud](https://streamlit.io/cloud)

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

#### Run the Pipeline Locally
   ```bash
   # Run the Screener & Inference
   uv run python -m screener.scripts.predict_live_trades

   # Launch the Dashboard
   uv run streamlit run frontend/app.py
   ```
---

# GitHub Actions Deployment

The project runs on two separate schedules:

| Workflow | Schedule | Description |
| :--- | :--- | :--- |
| **Daily Screener** | Mon-Fri @ 9:45 AM PT | Fetches live data, screens options, uploads to S3 & Google Sheets. |
| **Weekly Labeling** | Mondays @ 6:30 AM PT | Checks past trades, labels outcomes, updates ML datasets. |
| **ML Inference** | On "Daily Screener" Success | Success	Downloads latest model, runs predictions, and uploads CSV reports. |

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
| `SUPABASE_URL`     | Your Supabase project URL              |
| `SUPABASE_SERVICE_ROLE_KEY`     | Admin key for writing data from GitHub Actions.             |

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
