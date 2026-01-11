# ML-Powered Covered Call Screener **Active Development**

An end-to-end data engineering and machine learning pipeline designed to identify, analyze, and predict high-probability covered call opportunities.

It fetches data from **Yahoo Finance**, calculates metrics (Greeks, Risk/Reward, Annualized Return), and filters based on risk management criteria.

The results are automatically processed and uploaded to an **AWS S3** Data Lake as Parquet files for historical analysis and backtesting.

## Features

* **Multi-Source Data:** Integrates Yahoo Finance (market data) and FRED (Risk-Free Rate).
* **Analytics:** Calculates Black-Scholes Greeks (Delta, Gamma, Theta, Vega) and proprietary risk metrics.
* **Filtering:** Filters options based on Volume, Open Interest, Delta, Return on Risk and other metrics.
* **Cloud Native:** Designed to run on **GitHub Actions** (scheduled M-F) and export to **AWS S3**.

---

## Setup & Installation

### Prerequisites
* Python 3.13+
* [`uv`](https://github.com/astral-sh/uv)
* [AWS Account](https://aws.amazon.com/s3/) (S3 bucket)
* [FRED API Key](https://fred.stlouisfed.org/docs/api/api_key.html)

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

### Run Locally
   ```bash
   uv run main.py
   ```

---

# GitHub Actions Deployment

This project is configured to run automatically **every weekday at 10:00 AM Pacific Time**.

- Automatic: Runs Mon-Fri at 18:00 UTC (10:00 AM PST / 11:00 AM PDT).

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
| `CONFIG_FILE_BASE64`     | **Crucial:** Your `config.py` file encoded as a Base64 string.              |

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

