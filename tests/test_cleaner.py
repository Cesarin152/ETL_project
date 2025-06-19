import pandas as pd
import pytest
from etl.cleaner import fill_missing_values, remove_outliers


def test_fill_missing_mean():
    data = {'cat': ['a', 'a', 'b', 'b'], 'val': [1, None, 3, None]}
    df = pd.DataFrame(data)
    result = fill_missing_values(df, method='mean', category_col='cat')
    assert result['val'].isnull().sum() == 0
    assert result.loc[1, 'val'] == 1.0
    assert result.loc[3, 'val'] == 3.0


def test_remove_outliers():
    df = pd.DataFrame({'x': [10, 12, 11, 9, 3000]})
    clean_df = remove_outliers(df, 'x', threshold=2)
    assert 3000 not in clean_df['x'].values
    assert len(clean_df) == 4
