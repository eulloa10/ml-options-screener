import logging
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared import config
from shared.option_greeks import OptionGreeks
from shared.models import ScreenerCriteria
from services.market_data import MarketDataService
from services.storage import StorageService

log_dir = 'option_screener_logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'option_screener.log')),
        logging.StreamHandler(sys.stdout)
    ]
)

class OptionScreener:
    def __init__(self, stocks=None, criteria=None):
        load_dotenv()

        self.stocks = stocks or config.STOCKS
        self.criteria = criteria or ScreenerCriteria()
        self.col_names = config.COLUMN_NAMES
        self.cols_to_drop = config.COLUMNS_TO_DROP
        self.final_col_order = config.FINAL_COLUMN_ORDER
        self.market_data = MarketDataService(fred_api_key=os.getenv('FRED_API_KEY'))
        self.storage = StorageService(bucket_name=os.getenv('S3_BUCKET_NAME'))
        
        logging.info("OptionScreener initialized.")

    def process_single_date(self, ticker, date, stock_info, risk_free_rate, today):
        """
        Calculates Greeks and Metrics for a specific expiration date.
        """
        try:
            calls = self.market_data.get_option_chain_for_date(ticker, date)
            
            if calls is None or calls.empty: 
                return None

            S = stock_info['stock_price']
            if S == 0: return None

            expiry_date = datetime.strptime(date, '%Y-%m-%d')
            days_to_expiry = (expiry_date - today).days
            T = max(days_to_expiry, 1) / 365
            dte_adj = max(days_to_expiry, 1)

            try:
                greeks = OptionGreeks.calculate_greeks_vectorized(
                    S=S, K=calls['strike'].values, T=T,
                    r=risk_free_rate, sigma=calls['impliedVolatility'].values
                )
            except Exception:
                greeks = pd.DataFrame(0, index=calls.index, columns=['delta', 'gamma', 'theta', 'vega', 'rho'])

            premium_return = (calls['lastPrice'] / S) * 100
            annualized_return = premium_return * (365 / dte_adj)
            out_of_the_money = ((calls['strike'] - S) / S) * 100
            max_gain = calls['lastPrice'] * 100
            max_loss = (S - calls['lastPrice']) * 100
            break_even = S - calls['lastPrice']
            loss_adj = max_loss.replace(0, 1)
            risk_reward_ratio = max_gain / loss_adj
            return_per_day = premium_return / dte_adj

            metrics_data = {
                'expiration_date': date,
                'days_to_expiry': days_to_expiry,
                'premium_return': premium_return,
                'annualized_return': annualized_return,
                'out_of_the_money': out_of_the_money,
                'max_gain': max_gain,
                'max_loss': max_loss,
                'break_even': break_even,
                'risk_reward_ratio': risk_reward_ratio,
                'return_per_day': return_per_day
            }
            
            metrics = pd.DataFrame(metrics_data, index=calls.index)
            final_df = pd.concat([calls, greeks, metrics], axis=1)
            return final_df
        except Exception:
            return None

    def get_option_chain(self, ticker):
        """
        Orchestrates fetching dates, filtering them, and processing chains for a single ticker.
        """
        try:
            stock_info = self.stock_metadata_map.get(ticker)
            risk_free_rate = self.market_data.get_risk_free_rate()
            
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            min_bound = today + timedelta(days=self.criteria.min_days)
            max_bound = today + timedelta(days=self.criteria.max_days)
            
            available_dates = self.market_data.get_expiration_dates(ticker)
            
            valid_dates = []
            for d in available_dates:
                dt_obj = datetime.strptime(d, '%Y-%m-%d')
                if min_bound <= dt_obj <= max_bound:
                    valid_dates.append(d)
            
            if not valid_dates: return None

            with ThreadPoolExecutor(max_workers=min(len(valid_dates), 5)) as executor:
                futures = [
                    executor.submit(
                        self.process_single_date, 
                        ticker,
                        date, 
                        stock_info, 
                        risk_free_rate, 
                        today
                    ) for date in valid_dates
                ]
                all_calls = [f.result() for f in futures if f.result() is not None]

            if all_calls:
                combined = pd.concat(all_calls, ignore_index=True)
                combined = (combined
                            .rename(columns=self.col_names)
                            .drop(columns=self.cols_to_drop, errors='ignore'))

                meta_df = pd.DataFrame([stock_info] * len(combined), index=combined.index)
                cols_to_use = meta_df.columns.difference(combined.columns)
                final_combined = pd.concat([combined, meta_df[cols_to_use]], axis=1)
                
                return final_combined

            return None
        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")
            return None

    def screen_options(self):
        """
        Main Routine: Fetches metadata, iterates stocks, and filters results.
        """
        try:
            print("Fetching stock metadata...")
            self.stock_metadata_map = self.market_data.get_stock_metadata(self.stocks)
            print(f"Metadata fetched for {len(self.stock_metadata_map)} stocks.")
            
            today = datetime.now()
            min_date = (today + timedelta(days=self.criteria.min_days)).strftime('%Y-%m-%d')
            max_date = (today + timedelta(days=self.criteria.max_days)).strftime('%Y-%m-%d')
            print(f"Scanning option chains expiring between {min_date} and {max_date}...")
            
            results = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(self.get_option_chain, t): t 
                    for t in self.stocks if t in self.stock_metadata_map
                }
                for f in futures:
                    res = f.result()
                    if res is not None: results.append(res)

            if not results:
                print("No option chains found in this date range.")
                return pd.DataFrame()
                
            df = pd.concat(results, ignore_index=True)
            print(f"DEBUG: Found {len(df)} total option rows BEFORE filtering.")

            c = self.criteria
            query_str = (
                "strike >= stock_price and volume >= @c.min_volume and premium >= @c.min_premium and "
                "delta.between(@c.min_delta, @c.max_delta) and "
                "vega.between(@c.min_vega, @c.max_vega) and "
                "(pe_ratio == 0 or pe_ratio.between(@c.min_pe_ratio, @c.max_pe_ratio)) and "
                "stock_price.between(@c.min_stock_price, @c.max_stock_price) and "
                "open_interest >= @c.min_open_interest and "
                "implied_volatility.between(@c.min_implied_volatility, @c.max_implied_volatility)"
            )
            
            try:
                filtered = df.query(query_str).copy()
            except Exception as e:
                print(f"Query Syntax Error: {e}")
                return pd.DataFrame()

            print(f"DEBUG: Found {len(filtered)} rows AFTER filtering.")

            # Diagnostics
            if filtered.empty and not df.empty:
                print("\n--- DEBUG: FILTER DIAGNOSTICS (Rows Passing) ---")
                print(f"Volume (>={c.min_volume}): {len(df[df['volume'] >= c.min_volume])}")
                print(f"Premium (>={c.min_premium}): {len(df[df['premium'] >= c.min_premium])}")
                print(f"Delta ({c.min_delta}-{c.max_delta}): {len(df[df['delta'].between(c.min_delta, c.max_delta)])}")
                print("------------------------------------------\n")

            if not filtered.empty:
                for col in self.final_col_order:
                    if col not in filtered.columns:
                        filtered[col] = None
                
                return filtered[self.final_col_order].sort_values(
                    ['premium_return', 'days_to_expiry'], 
                    ascending=[False, True]
                )

            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Error in screen_options: {e}")
            return pd.DataFrame()

    def export_to_s3(self):
        """
        Runs the screen and uses StorageService to upload the result.
        """
        try:
            results = self.screen_options()
            if results.empty:
                print("No opportunities to export.")
                return

            today_str = datetime.now().strftime('%Y-%m-%d')
            results['snapshot_date'] = pd.to_datetime(today_str)

            file_key = f"raw_data/{today_str}.parquet"

            self.storage.upload_parquet(results, file_key)
            
            print(f"Success: Data uploaded to S3: .../{file_key}")

        except Exception as e:
            logging.error(f"Error exporting to S3: {e}")
            print("To debug detailed stack traces, run this script locally.")
