# Your stock list for option screening
STOCKS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']  # Add more stock symbols as needed

# Screening criteria thresholds
MIN_VOLUME = 100
MIN_PREMIUM = 0.3
MIN_DAYS_TO_EXPIRY = 7
MAX_DAYS_TO_EXPIRY = 10
MIN_PE_RATIO = 8
MAX_PE_RATIO = 50
MIN_IMPLIED_VOLATILITY = 0.3
MAX_IMPLIED_VOLATILITY = 1.0
MIN_STOCK_PRICE = 10.0
MAX_STOCK_PRICE = 1000.0
MIN_DELTA = 0.2
MAX_DELTA = 0.5
MIN_GAMMA = 0
MAX_GAMMA = 0.1
MIN_THETA = -1.0
MAX_THETA = -0.1
MIN_VEGA = -1.0
MAX_VEGA = 0.5
MIN_OPEN_INTEREST = 100

# Data processing configurations
COLUMN_NAMES = {
    'contractSymbol': 'contract_name',
    'lastTradeDate': 'last_trade_date',
    'strike': 'strike',
    'lastPrice': 'premium',
    'impliedVolatility': 'implied_volatility',
    'inTheMoney': 'in_the_money',
    'contractSize': 'contract_size',
    'percentChange': 'percent_change',
    'openInterest': 'open_interest',
}

# Columns to drop from the dataset
COLUMNS_TO_DROP = [
    'currency'
]

# Final column order for the output dataset
FINAL_COLUMN_ORDER = [
    'company_name', 'ticker', 'contract_name',
    'expiration_date', 'last_trade_date', 'stock_price',
    'strike', 'premium', 'bid',
    'ask', 'change', 'percent_change',
    'volume',	'open_interest', 'implied_volatility',
    'delta',	'gamma', 'theta',
    'vega', 'rho', 'days_to_expiry',
    'contract_size',	'premium_return', 'annualized_return',
    'out_of_the_money', 'max_gain', 'max_loss',
    'break_even',	'risk_reward_ratio', 'return_per_day',
    'in_the_money', 'pe_ratio', 'stock_volume',
    'stock_average_volume', 'market_cap',	'stock_beta',
    'industry', 'average_analyst_rating',	'earnings_date',
    'dividend_date', 'dividend_yield'
]
