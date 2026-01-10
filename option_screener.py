import logging
import os
import warnings
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

import pandas as pd
import yfinance as yf
from fredapi import Fred
from dotenv import load_dotenv

# --- SUPPRESS INTERNAL WARNINGS ---
warnings.simplefilter(action='ignore', category=FutureWarning)
# ----------------------------------

# Internal Imports
from option_greeks import OptionGreeks
from models import ScreenerCriteria
import config

# Logging Configuration
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

                # Get Price
                price = info.get('regularMarketPrice') or info.get('currentPrice') or info.get('navPrice')
                if not price: continue

                # Get PE (Handle ETFs)
                pe = info.get('trailingPE', 0.0)

                # Get Earnings (Robust)
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

            # Calculations
            premium_return = (calls['lastPrice'] / S) * 100
            annualized_return = premium_return * (365 / dte_adj)
            out_of_the_money = ((calls['strike'] - S) / S) * 100
            max_gain = calls['lastPrice'] * 100
            max_loss = (S - calls['lastPrice']) * 100
            break_even = S - calls['lastPrice']
            loss_adj = max_loss.replace(0, 1)
            risk_reward_ratio = max_gain / loss_adj
            return_per_day = premium_return / dte_adj

            # Dictionary Construction (Pandas 3.0 Safe)
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

                # Broadcast stock metadata
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
            
            # Robust Query
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

    def export_to_excel(self):
        try:
            results = self.screen_options()
            if results.empty:
                print("No opportunities to export.")
                return

            today_str = datetime.now().strftime('%Y_%m_%d')
            filename = f'covered_call_opportunities_{today_str}.xlsx'

            clean_data = {}
            for col in results.columns:
                series = results[col]
                if pd.api.types.is_datetime64_any_dtype(series):
                    if series.dt.tz is not None:
                        clean_data[col] = series.dt.tz_localize(None)
                    else:
                        clean_data[col] = series
                else:
                    clean_data[col] = series
            
            export_df = pd.DataFrame(clean_data)
            cols_to_use = [c for c in self.final_col_order if c in export_df.columns]
            export_df = export_df[cols_to_use]

            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                export_df.to_excel(writer, sheet_name='Opportunities', index=False)
                workbook, worksheet = writer.book, writer.sheets['Opportunities']
                
                money_fmt = workbook.add_format({'num_format': '#,##0.00'})
                pct_fmt = workbook.add_format({'num_format': '0.00%'})
                pct_sign_only_fmt = workbook.add_format({'num_format': '0.00"%"'})
                greek_fmt = workbook.add_format({'num_format': '0.0000'})
                comma_fmt = workbook.add_format({'num_format': '#,##0'})

                for idx, col in enumerate(export_df.columns):
                    try:
                        col_len = export_df[col].astype(str).map(len).max()
                    except:
                        col_len = 10
                    max_len = max(col_len, len(str(col))) + 2

                    if col in ['premium', 'stock_price', 'strike', 'max_gain', 'max_loss', 'break_even', 'bid', 'ask', 'change']:
                        worksheet.set_column(idx, idx, 12, money_fmt)
                    elif col in ['implied_volatility']:
                        worksheet.set_column(idx, idx, 12, pct_fmt)
                    elif col in ['premium_return', 'annualized_return', 'out_of_the_money', 'percent_change']:
                        worksheet.set_column(idx, idx, 12, pct_sign_only_fmt)
                    elif col in ['delta', 'gamma', 'theta', 'vega', 'rho']:
                        worksheet.set_column(idx, idx, 12, greek_fmt)
                    elif col in ['market_cap', 'open_interest', 'volume', 'stock_volume', 'stock_average_volume']:
                        worksheet.set_column(idx, idx, 12, comma_fmt)
                    else:
                        worksheet.set_column(idx, idx, max_len)

            print(f"Results exported successfully to {filename}")
        except Exception as e:
            logging.error(f"Error exporting to Excel: {e}")
            print(f"Error: {e}")

def main():
    try:
        screener = OptionScreener()
        screener.export_to_excel()
    except Exception as e:
        print(f"Main Error: {e}")

if __name__ == "__main__":
    main()
