import pytest
import pandas as pd

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'Date': ['2024-01-01', '2024-01-02'],
        'Time': ['10:00:00', '12:00:00'],
        'Value': [1.2, None]
    })
