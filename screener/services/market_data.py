import yfinance as yf
from fredapi import Fred
from functools import lru_cache
import logging

class MarketDataService:
    def __init__(self, fred_api_key):
        self.fred_api_key = fred_api_key

    @lru_cache(maxsize=1)
    def get_risk_free_rate(self):
        try:
            fred = Fred(api_key=self.fred_api_key)
            treasury_rate = fred.get_series('DTB3')
            current_rate = treasury_rate.iloc[-1] / 100
            return current_rate
        except Exception as e:
            logging.error(f"Error getting risk-free rate from FRED. Using default 4.25%")
            return 0.0425 

    def get_stock_metadata(self, tickers):
        batch_info = {}
        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                
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
        
    def get_expiration_dates(self, ticker):
        """
        Returns a list of expiration dates for a ticker.
        """
        try:
            return yf.Ticker(ticker).options
        except Exception as e:
            logging.error(f"Error fetching expiration dates for {ticker}: {e}")
            return []

    def get_option_chain_for_date(self, ticker, date):
        """
        Returns the CALLS dataframe for a specific ticker and date.
        """
        try:
            return yf.Ticker(ticker).option_chain(date).calls
        except Exception:
            return None
