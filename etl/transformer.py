# etl/transformer.py

import pandas as pd

class Transformer:
    @staticmethod
    def standardize_datetime(df, date_col='Date', time_col='Time', datetime_col='DateTime') -> pd.DataFrame:
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

        if time_col in df.columns:
            common_formats = ['%I:%M %p', '%H:%M:%S', '%H:%M']
            for fmt in common_formats:
                temp = pd.to_datetime(df[time_col], format=fmt, errors='coerce')
                if temp.notna().sum() > 0.8 * df.shape[0]:
                    df[time_col] = temp.dt.time
                    break
            else:
                df[time_col] = pd.to_datetime(df[time_col], errors='coerce').dt.time

            df[datetime_col] = pd.to_datetime(df[date_col].astype(str) + " " + df[time_col].astype(str), errors='coerce')
            df.drop(columns=[date_col, time_col], inplace=True)
        else:
            df[datetime_col] = df[date_col]

        return df

    @staticmethod
    def expand_datetime(df, datetime_col='DateTime', up_to='minute') -> pd.DataFrame:
        levels = ['year', 'month', 'day', 'hour', 'minute', 'second']
        extractors = {
            'year': lambda x: x.dt.year,
            'month': lambda x: x.dt.month,
            'day': lambda x: x.dt.day,
            'hour': lambda x: x.dt.hour,
            'minute': lambda x: x.dt.minute,
            'second': lambda x: x.dt.second,
        }

        if datetime_col not in df.columns:
            raise ValueError(f"Columna '{datetime_col}' no encontrada.")

        df = df.copy()
        df[datetime_col] = pd.to_datetime(df[datetime_col], errors='coerce')

        for level in levels[:levels.index(up_to) + 1]:
            df[level] = extractors[level](df[datetime_col])

        return df

    @staticmethod
    def rename_columns(df: pd.DataFrame, rename_dict: dict) -> pd.DataFrame:
        return df.rename(columns=rename_dict)

    @staticmethod
    def calculate_keys(df: pd.DataFrame, datetime_col='DateTime') -> pd.DataFrame:
        df = df.copy()
        dt = df[datetime_col]
        df["key"] = dt.dt.strftime('%d_%m_%Y') + "_" + dt.dt.hour.astype(str).str.zfill(2)
        df["key_m"] = dt.dt.strftime('%d_%m_%Y') + "_" + dt.dt.hour.astype(str).str.zfill(2) + "_" + dt.dt.minute.astype(str).str.zfill(2)
        df["key_month"] = dt.dt.strftime('%m_%Y')
        return df

    @staticmethod
    def convert_units(df: pd.DataFrame, columns: list[str], factor: float = 1000.0) -> pd.DataFrame:
        df = df.copy()
        for col in columns:
            if col in df.columns:
                df[col] = df[col] / factor
        return df

    @staticmethod
    def melt_to_long(df: pd.DataFrame, id_vars: list[str], value_vars: list[str]) -> pd.DataFrame:
        df = df.copy()
        df = df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='plant_metric',
            value_name='Value'
        )
        df[['Plant', 'Metric']] = df['plant_metric'].str.extract(r'(UP\d+)_(.*)')
        df.drop(columns='plant_metric', inplace=True)
        return df

    # ------------------------------------------------------------------
    # Métodos de instancia utilizados por el pipeline
    # ------------------------------------------------------------------

    def combine_pvsyst(self, dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """Une una lista de DataFrames de PVSyst en uno solo."""
        return pd.concat(dfs, ignore_index=True)

    def generate_keys(self, data_dict: dict) -> dict:
        """Genera columnas de claves para todos los DataFrames que contengan
        ``DateTime``."""
        for key, df in data_dict.items():
            if isinstance(df, pd.DataFrame) and 'DateTime' in df.columns:
                data_dict[key] = self.calculate_keys(df, datetime_col='DateTime')
        return data_dict

    def convert_units(self, df: pd.DataFrame, columns: list[str] | None = None, factor: float = 1000.0) -> pd.DataFrame:
        """Convierte unidades de energía de kWh a MWh.

        Si ``columns`` es ``None`` se detectarán automáticamente todas las
        columnas que contengan ``_MWh`` o ``_MVArh``.
        """
        if columns is None:
            columns = [c for c in df.columns if c.endswith('_MWh') or c.endswith('_MVArh')]
        return self.__class__.convert_units(df, columns=columns, factor=factor)

    def merge_energy_meteo(self, energia: pd.DataFrame, meteo: pd.DataFrame) -> pd.DataFrame:
        """Une los DataFrames de energía y meteorología por ``DateTime``."""
        return pd.merge(energia, meteo, on='DateTime')

    def melt_energy(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convierte a formato largo los datos de energía."""
        value_vars = [c for c in df.columns if c.endswith('_MWh') or c.endswith('_MVArh')]
        id_vars = [c for c in df.columns if c not in value_vars]
        return self.melt_to_long(df, id_vars=id_vars, value_vars=value_vars)

    def melt_pvsyst(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convierte a formato largo los datos de PVSyst."""
        value_vars = [c for c in df.columns if c.endswith('_E')]
        id_vars = [c for c in df.columns if c not in value_vars]
        return self.melt_to_long(df, id_vars=id_vars, value_vars=value_vars)


# Alias para mantener compatibilidad con el código existente
DataTransformer = Transformer


def standardize_datetime(df, date_col='Date', time_col='Time', datetime_col='DateTime') -> pd.DataFrame:
    """Wrapper para compatibilidad con los tests."""
    return DataTransformer.standardize_datetime(df, date_col=date_col, time_col=time_col, datetime_col=datetime_col)


def expand_datetime(df, datetime_col='DateTime', up_to='minute') -> pd.DataFrame:
    """Wrapper para compatibilidad con los tests."""
    return DataTransformer.expand_datetime(df, datetime_col=datetime_col, up_to=up_to)
