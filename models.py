from dataclasses import dataclass
import config

@dataclass
class ScreenerCriteria:
    min_volume: int = config.MIN_VOLUME
    min_open_interest: int = config.MIN_OPEN_INTEREST
    min_premium: float = config.MIN_PREMIUM
    min_days: int = config.MIN_DAYS_TO_EXPIRY
    max_days: int = config.MAX_DAYS_TO_EXPIRY
    min_delta: float = config.MIN_DELTA
    max_delta: float = config.MAX_DELTA
    min_gamma: float = config.MIN_GAMMA
    max_gamma: float = config.MAX_GAMMA
    min_theta: float = config.MIN_THETA
    max_theta: float = config.MAX_THETA
    min_vega: float = config.MIN_VEGA
    max_vega: float = config.MAX_VEGA
    min_implied_volatility: float = config.MIN_IMPLIED_VOLATILITY
    max_implied_volatility: float = config.MAX_IMPLIED_VOLATILITY
    min_pe_ratio: float = config.MIN_PE_RATIO
    max_pe_ratio: float = config.MAX_PE_RATIO
    min_stock_price: float = config.MIN_STOCK_PRICE
    max_stock_price: float = config.MAX_STOCK_PRICE
