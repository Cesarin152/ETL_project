import pandas as pd
import pytest
import importlib
from etl import mappings
mappings.rename_dicts = {'energia': {}, 'meteo': {}, 'pvsyst': {}}
import etl.pipeline as pipeline_module
importlib.reload(pipeline_module)
from etl.pipeline import ETLPipeline


class DummyFileSource:
    def load_excel(self, path):
        if path.endswith('.xlsx'):
            return pd.read_excel(path)
        return pd.read_csv(path)


class DummyDB:
    def __init__(self):
        self.inserted = {}

    def insert_dataframe(self, df, table_name, if_exists="append"):
        self.inserted[table_name] = df.copy()


class DummyCleaner:
    def fill_missing(self, df, method='mean', category_col=None):
        return df

    def fix_negatives(self, df, columns=None):
        return df

    def remove_outliers(self, df, column, threshold=3.0):
        return df


class DummyTransformer(pipeline_module.DataTransformer):
    def combine_pvsyst(self, dfs):
        return pd.concat(dfs, ignore_index=True)

    def generate_keys(self, data_dict):
        for k, df in data_dict.items():
            if isinstance(df, pd.DataFrame) and 'DateTime' in df.columns:
                data_dict[k] = self.calculate_keys(df, datetime_col='DateTime')
        return data_dict

    def convert_units(self, df):
        return df

    def merge_energy_meteo(self, energia, meteo):
        return pd.merge(energia, meteo, on='DateTime')

    def melt_energy(self, df):
        id_vars = [c for c in df.columns if c != 'UP1_Act_MWh']
        return self.melt_to_long(df, id_vars=id_vars, value_vars=['UP1_Act_MWh'])

    def melt_pvsyst(self, df):
        id_vars = [c for c in df.columns if c != 'UP1_E']
        return self.melt_to_long(df, id_vars=id_vars, value_vars=['UP1_E'])


@pytest.fixture
def sample_files(tmp_path):
    cms = pd.DataFrame({'Fecha': ['2024-01-01'], 'hora': ['00:00:00'], 'A': [1]})
    energia = pd.DataFrame({'Date': ['2024-01-01'], 'Time': ['00:00:00'], 'UP1_Act_MWh': [1000]})
    meteo = pd.DataFrame({'Date': ['2024-01-01'], 'Time': ['00:00:00'], 'Temp': [30]})
    pvsyst = pd.DataFrame({'Date': ['2024-01-01'], 'Time': ['00:00:00'], 'UP1_E': [10]})

    paths = {}
    for name, df in [
        ('cms', cms),
        ('energia', energia),
        ('meteo', meteo),
        ('up1_pvsyst', pvsyst),
        ('up2_pvsyst', pvsyst),
        ('up3_pvsyst', pvsyst),
        ('up4_pvsyst', pvsyst),
    ]:
        file_path = tmp_path / f"{name}.csv"
        df.to_csv(file_path, index=False)
        paths[name] = str(file_path)
    return paths


def test_pipeline_run(sample_files, monkeypatch):
    monkeypatch.setattr(pipeline_module, 'FileSource', lambda: DummyFileSource())
    monkeypatch.setattr(pipeline_module, 'DatabaseLoader', lambda: DummyDB())
    monkeypatch.setattr(pipeline_module, 'DataCleaner', DummyCleaner)
    monkeypatch.setattr(pipeline_module, 'DataTransformer', DummyTransformer)
    monkeypatch.setattr(pipeline_module, 'rename_dicts', {'energia': {}, 'meteo': {}, 'pvsyst': {}})

    pipeline = ETLPipeline(sample_files)
    pipeline.run()

    energia_long = pipeline.data['energia_long']
    pvsyst_long = pipeline.data['pvsyst_long']

    assert 'Plant' in energia_long.columns
    assert 'Metric' in pvsyst_long.columns
    assert pipeline.db.inserted['energia_consolidada'].equals(energia_long)
    assert pipeline.db.inserted['pvsyst_datos'].equals(pvsyst_long)
