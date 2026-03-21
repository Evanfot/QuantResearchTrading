import numpy as np
from tinycta.signal import osc


def ewmac(returns_adj, param, vola_span=20):
    fast = param
    slow = fast * 4
    return np.tanh(returns_adj.cumsum().apply(osc, args=(fast, slow, True))).values


def breakout(prices, param):
    roll_min = prices.rolling(param).min()
    roll_max = prices.rolling(param).max()
    roll_mean = (roll_min + roll_max) / 2
    return np.tanh((prices - roll_mean) / (roll_max - roll_min)).values


def scaled_bollinger(price, param=20, scalar=2, forecast_cap=10):
    ma = price.rolling(param).mean()
    std = price.rolling(param).std()
    z = (price - ma) / std
    forecast = np.tanh(z / scalar)
    return forecast.values


def carry_signal(
    funding_daily, returns, funding_span=8, vol_span=32, norm_span=60, eps=1e-8
):
    """
    funding_daily: pd.Series (daily funding rate, summed)
    returns: pd.Series (daily log returns)
    """
    # 1) Smooth funding
    smoothed_funding = funding_daily.ewm(span=funding_span, adjust=False).mean()
    # 1.2) invert for carry signal
    carry_economic = -1 * smoothed_funding
    # 2) Vol estimate
    vol = returns.ewm(span=vol_span, adjust=False).std()
    raw_carry = carry_economic / (vol + eps)
    # 3) Normalize which means will take more exposure when carry is strong less when weak/noisy
    scale = raw_carry.abs().ewm(span=norm_span, adjust=False).mean()
    # carry = np.tanh(raw_carry / (scale + eps)).values
    carry = (raw_carry / (scale + eps)).clip(-5, 5).values
    return carry
