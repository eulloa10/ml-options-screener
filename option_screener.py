import io
import logging
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

import boto3
import pandas as pd
import yfinance as yf
from fredapi import Fred
from dotenv import load_dotenv


from option_greeks import OptionGreeks
from models import ScreenerCriteria
import config

logging.basicConfig(
    filename='option_screener_logs/option_screener.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class OptionScreener:
    def __init__(self, stocks=None, criteria=None):
        load_dotenv()
        self.stocks = stocks or config.STOCKS
        self.criteria = criteria or ScreenerCriteria()
        self.col_names = config.COLUMN_NAMES
        self.cols_to_drop = config.COLUMNS_TO_DROP
        self.final_col_order = config.FINAL_COLUMN_ORDER
        self.fred_api_key = os.getenv('FRED_API_KEY')
        logging.info("OptionScreener initialized in Robust Mode.")

    @lru_cache(maxsize=1)
    def get_risk_free_rate(self):
        try:
            fred = Fred(api_key=self.fred_api_key)
            treasury_rate = fred.get_series('DTB3')
            current_rate = treasury_rate.iloc[-1] / 100
            return current_rate
        except Exception as e:
            logging.error(f"Error getting risk-free rate: {e}")
            return 0.0425 

    def _get_earnings_robust(self, stock):
        """
        Attempts to fetch the next earnings date using 3 different yfinance methods.
        Returns 'N/A' if all fail.
        """
        try:
            # METHOD 1: .calendar attribute (Old standard)
            cal = stock.calendar
            if cal is not None and not isinstance(cal, list): # Check if it's a valid dict/df
                if isinstance(cal, dict) and 'Earnings Date' in cal:
                    dates = cal['Earnings Date']
                    if dates:
                        return dates[0].strftime('%Y/%m/%d')
                elif hasattr(cal, 'loc') and 'Earnings Date' in cal.index:
                    return cal.loc['Earnings Date'].iloc[0].strftime('%Y/%m/%d')

            # METHOD 2: .get_calendar() (Newer method)
            try:
                cal_dict = stock.get_calendar()
                if cal_dict and isinstance(cal_dict, dict):
                    if 'Earnings Date' in cal_dict:
                        dates = cal_dict['Earnings Date']
                        if dates:
                            return dates[0].strftime('%Y/%m/%d')
            except:
                pass

            # METHOD 3: .earnings_dates (DataFrame lookup)
            # This is the most reliable for future dates if the others fail.
            try:
                dates_df = stock.earnings_dates
                if dates_df is not None and not dates_df.empty:
                    # Filter for future dates only
                    now = pd.Timestamp.now().tz_localize(dates_df.index.tz)
                    future_dates = dates_df[dates_df.index > now].sort_index()
                    if not future_dates.empty:
                        return future_dates.index[0].strftime('%Y/%m/%d')
            except:
                pass
            
            return 'N/A'

        except Exception as e:
            # logging.debug(f"Earnings fetch failed: {e}")
            return 'N/A'

    def prefetch_stock_metadata(self):
        batch_info = {}
        for ticker in self.stocks:
            try:
                stock = yf.Ticker(ticker)
                
                # Wrap info fetch to catch 404s
                try:
                    info = stock.info
                except Exception:
                    logging.warning(f"Could not fetch info for {ticker}")
                    continue

                if not info: continue

                price = info.get('regularMarketPrice') or info.get('currentPrice') or info.get('navPrice')
                if not price: continue

                pe = info.get('trailingPE', 0.0)

                next_earnings = self._get_earnings_robust(stock)

                div_date = info.get('dividendDate')
                div_formatted = pd.to_datetime(div_date, unit='s').strftime('%m/%d/%Y') if div_date else 'N/A'

                batch_info[ticker] = {
                    'ticker': ticker,
                    'company_name': info.get('shortName', 'Unknown'),
                    'stock_price': price,
                    'pe_ratio': pe, 
                    'stock_volume': info.get('volume', 0),
                    'stock_average_volume': info.get('averageVolume', 0),
                    'market_cap': info.get('marketCap', 0),
                    'stock_beta': info.get('beta', 0),
                    'industry': info.get('industry', 'ETF' if info.get('quoteType') == 'ETF' else 'Unknown'),
                    'average_analyst_rating': info.get('averageAnalystRating', 'Unknown'),
                    'earnings_date': next_earnings,
                    'dividend_date': div_formatted,
                    'dividend_yield': info.get('dividendYield', 0)
                }
            except Exception as e:
                logging.debug(f"Metadata fetch failed for {ticker}: {e}")
        return batch_info

    def process_single_date(self, stock, date, stock_info, risk_free_rate, today):
        try:
            opt_chain = stock.option_chain(date)
            calls = opt_chain.calls.copy()
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
            except:
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
        try:
            stock_info = self.stock_metadata_map.get(ticker)
            stock = yf.Ticker(ticker)
            risk_free_rate = self.get_risk_free_rate()
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

            min_bound = today + timedelta(days=self.criteria.min_days)
            max_bound = today + timedelta(days=self.criteria.max_days)
            
            available_dates = stock.options
            valid_dates = []
            for d in available_dates:
                dt_obj = datetime.strptime(d, '%Y-%m-%d')
                if min_bound <= dt_obj <= max_bound:
                    valid_dates.append(d)
            
            if not valid_dates: return None

            with ThreadPoolExecutor(max_workers=min(len(valid_dates), 5)) as executor:
                futures = [executor.submit(self.process_single_date, stock, date, stock_info, risk_free_rate, today) for date in valid_dates]
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
        try:
            print("Fetching stock metadata...")
            self.stock_metadata_map = self.prefetch_stock_metadata()
            print(f"Metadata fetched for {len(self.stock_metadata_map)} stocks.")
            
            today = datetime.now()
            min_date = (today + timedelta(days=self.criteria.min_days)).strftime('%Y-%m-%d')
            max_date = (today + timedelta(days=self.criteria.max_days)).strftime('%Y-%m-%d')
            print(f"Scanning option chains expiring between {min_date} and {max_date}...")
            
            results = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(self.get_option_chain, t): t for t in self.stocks if t in self.stock_metadata_map}
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

            if filtered.empty and not df.empty:
                print("\n--- DEBUG: FILTER DIAGNOSTICS (Rows Passing) ---")
                print(f"Volume (>={c.min_volume}): {len(df[df['volume'] >= c.min_volume])}")
                print(f"Premium (>={c.min_premium}): {len(df[df['premium'] >= c.min_premium])}")
                print(f"Delta ({c.min_delta}-{c.max_delta}): {len(df[df['delta'].between(c.min_delta, c.max_delta)])}")
                print(f"PE Ratio ({c.min_pe_ratio}-{c.max_pe_ratio} OR 0): {len(df[(df['pe_ratio'] == 0) | df['pe_ratio'].between(c.min_pe_ratio, c.max_pe_ratio)])}")
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
            Runs the screen and uploads the result directly to AWS S3 as a Parquet file.
            """
            try:
                results = self.screen_options()
                if results.empty:
                    print("No opportunities to export.")
                    return

                today_str = datetime.now().strftime('%Y-%m-%d')
                results['snapshot_date'] = pd.to_datetime(today_str)

                parquet_buffer = io.BytesIO()
                results.to_parquet(parquet_buffer, index=False)
                parquet_buffer.seek(0)

                bucket_name = os.getenv('S3_BUCKET_NAME')
                if not bucket_name:
                    raise ValueError("S3_BUCKET_NAME is missing from .env file")

                file_key = f"raw_data/{today_str}.parquet"

                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
                )

                s3_client.put_object(
                    Body=parquet_buffer.getvalue(), 
                    Bucket=bucket_name, 
                    Key=file_key
                )
                
                logging.info(f"Successfully uploaded {file_key} to s3://{bucket_name}")
                print(f"Success: Data uploaded to s3://{bucket_name}/{file_key}")

            except Exception as e:
                logging.error(f"Error exporting to S3: {e}")
                print(f"Error: {e}")
