# tests/test_indicators.py
import pandas as pd
import numpy as np
from bot import sma_series, ema_series, rsi_series, atr_series, pct_atr_last

def make_sample_df(n=100):
    # simple upward ramp + noise
    close = pd.Series(np.linspace(1.0, 2.0, n) + np.random.normal(0, 0.001, n))
    df = pd.DataFrame({
        "open_time": pd.date_range("2020-01-01", periods=n, freq="T", tz="UTC"),
        "open": close,
        "high": close * 1.001,
        "low": close * 0.999,
        "close": close,
        "volume": np.random.randint(1, 100, n)
    })
    return df

def test_sma_ema():
    df = make_sample_df(50)
    s = sma_series(df["close"], length=10)
    e = ema_series(df["close"], length=10)
    assert len(s) == len(df) and len(e) == len(df)
    # SMA should equal simple mean for first 10 last value
    assert not s.isnull().any()
    assert not e.isnull().any()

def test_rsi_range():
    df = make_sample_df(60)
    r = rsi_series(df["close"], length=14)
    assert len(r) == len(df)
    # RSI should be between 0 and 100
    assert (r.dropna() >= 0).all()
    assert (r.dropna() <= 100).all()

def test_atr_and_pct():
    df = make_sample_df(60)
    a = atr_series(df, length=14)
    p = pct_atr_last(df, length=14)
    assert len(a) == len(df)
    assert p >= 0
