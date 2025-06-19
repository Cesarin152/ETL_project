import pandas as pd
import pytest
from etl.transformer import standardize_datetime, expand_datetime


def test_standardize_datetime():
    df = pd.DataFrame({
        'Date': ['2024-01-01', '2024-01-02'],
        'Time': ['12:00:00', '14:30:00']
    })
    result = standardize_datetime(df, date_col='Date', time_col='Time')
    assert 'DateTime' in result.columns
    assert pd.api.types.is_datetime64_any_dtype(result['DateTime'])


def test_expand_datetime():
    df = pd.DataFrame({'DateTime': pd.to_datetime(['2024-01-01 10:15:00'])})
    result = expand_datetime(df, 'DateTime', up_to='minute')
    for col in ['year', 'month', 'day', 'hour', 'minute']:
        assert col in result.columns
