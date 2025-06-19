# etl/transformer.py

import pandas as pd

class DataTransformer:
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

    # Métodos adicionales utilizados por el pipeline
    @staticmethod
    def combine_pvsyst(dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """Une varios DataFrames de PVSyst en uno solo."""
        return pd.concat(dfs, ignore_index=True)

    @staticmethod
    def generate_keys(data: dict[str, pd.DataFrame], datetime_col: str = 'DateTime') -> dict[str, pd.DataFrame]:
        """Aplica calculate_keys a todos los DataFrames que contengan la columna dada."""
        result = {}
        for name, df in data.items():
            if datetime_col in df.columns:
                result[name] = DataTransformer.calculate_keys(df, datetime_col)
            else:
                result[name] = df
        return result

    @staticmethod
    def convert_units(df: pd.DataFrame, factor: float = 1000.0) -> pd.DataFrame:
        """Convierte columnas de energía de kWh a MWh dividiendo por el factor."""
        df = df.copy()
        cols = [c for c in df.columns if c.startswith('UP') and ('MWh' in c or 'kWh' in c)]
        for col in cols:
            df[col] = df[col] / factor
        return df

    @staticmethod
    def merge_energy_meteo(energia: pd.DataFrame, meteo: pd.DataFrame) -> pd.DataFrame:
        """Enriquece la energía con las variables meteorológicas."""
        return pd.merge(energia, meteo, on='DateTime', how='left')

    @staticmethod
    def melt_energy(df: pd.DataFrame) -> pd.DataFrame:
        cols = [c for c in df.columns if c.startswith('UP') and ('MWh' in c or 'MVArh' in c)]
        id_vars = [c for c in df.columns if c not in cols]
        return DataTransformer.melt_to_long(df, id_vars, cols)

    @staticmethod
    def melt_pvsyst(df: pd.DataFrame) -> pd.DataFrame:
        cols = [c for c in df.columns if c not in ['DateTime']]
        id_vars = ['DateTime'] if 'DateTime' in df.columns else []
        return DataTransformer.melt_to_long(df, id_vars, cols)


# Funciones utilitarias para compatibilidad con las pruebas
def standardize_datetime(df, date_col='Date', time_col='Time', datetime_col='DateTime') -> pd.DataFrame:
    return DataTransformer.standardize_datetime(df, date_col, time_col, datetime_col)


def expand_datetime(df, datetime_col='DateTime', up_to='minute') -> pd.DataFrame:
    return DataTransformer.expand_datetime(df, datetime_col, up_to)
