# option_greeks.py
import numpy as np
from scipy.stats import norm
import pandas as pd

class OptionGreeks:
    @staticmethod
    def calculate_greeks_vectorized(S, K, T, r, sigma):
        """
        Vectorized Greeks calculation for multiple options

        Parameters:
        -----------
        S : float or array-like
            Stock price(s)
        K : array-like
            Strike prices
        T : float
            Time to expiration in years
        r : float
            Risk-free rate
        sigma : array-like
            Implied volatilities
        """
        # Handle edge cases
        if T <= 0 or np.any(sigma <= 0):
            return pd.DataFrame({
                'delta': 0, 'gamma': 0, 'theta': 0, 'vega': 0, 'rho': 0
            })

        # Calculate d1 and d2
        sqrt_T = np.sqrt(T)
        d1 = (np.log(S/K) + (r + sigma**2/2)*T) / (sigma*sqrt_T)
        d2 = d1 - sigma*sqrt_T

        # Calculate Greeks
        delta = norm.cdf(d1)
        gamma = norm.pdf(d1)/(S*sigma*sqrt_T)
        theta = (-S*sigma*norm.pdf(d1))/(2*sqrt_T) - r*K*np.exp(-r*T)*norm.cdf(d2)
        theta = theta/365  # Convert to daily theta
        vega = S * sqrt_T * norm.pdf(d1) / 100  # For 1% change in volatility
        rho = K * T * np.exp(-r*T) * norm.cdf(d2) / 100  # For 1% change in rate

        return pd.DataFrame({
            'delta': delta,
            'gamma': gamma,
            'theta': theta,
            'vega': vega,
            'rho': rho
        })

    @staticmethod
    def interpret_greeks(greeks_df):
        """Add interpretations to Greeks values"""
        interpretations = pd.DataFrame()

        interpretations['delta_interpretation'] = greeks_df['delta'].apply(
            lambda x: 'Strong bullish' if x > 0.7 else
                     'Bullish' if x > 0.5 else
                     'Neutral' if x > 0.3 else 'Bearish'
        )

        interpretations['gamma_interpretation'] = greeks_df['gamma'].apply(
            lambda x: 'High sensitivity' if x > 0.1 else
                     'Moderate sensitivity' if x > 0.05 else
                     'Low sensitivity'
        )

        interpretations['theta_interpretation'] = greeks_df['theta'].apply(
            lambda x: 'High time decay' if x < -0.1 else
                     'Moderate time decay' if x < -0.05 else
                     'Low time decay'
        )

        return interpretations
