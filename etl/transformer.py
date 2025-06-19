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
    def convert_units(
        df: pd.DataFrame, columns: list[str] | None = None, factor: float = 1000.0
    ) -> pd.DataFrame:
        """Divide the selected numeric columns by ``factor`` (default 1000)."""
        df = df.copy()
        if columns is None:
            columns = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
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

    @staticmethod
    def combine_pvsyst(dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """Concatenate multiple PVSyst DataFrames adding a ``Plant`` column."""
        tagged = []
        for idx, df in enumerate(dfs, start=1):
            temp = df.copy()
            temp["Plant"] = f"UP{idx}"
            tagged.append(temp)
        return pd.concat(tagged, ignore_index=True)

    @staticmethod
    def generate_keys(data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Apply :func:`calculate_keys` to all DataFrames containing a date."""
        result = {}
        for name, df in data.items():
            if "DateTime" in df.columns:
                result[name] = Transformer.calculate_keys(df, "DateTime")
            elif "Date" in df.columns:
                temp = df.copy()
                temp["Date"] = pd.to_datetime(temp["Date"], errors="coerce")
                temp["key"] = temp["Date"].dt.strftime("%d_%m_%Y")
                temp["key_m"] = temp["Date"].dt.strftime("%d_%m_%Y_00_00")
                temp["key_month"] = temp["Date"].dt.strftime("%m_%Y")
                result[name] = temp
            else:
                result[name] = df
        return result

    @staticmethod
    def merge_energy_meteo(df_energy: pd.DataFrame, df_meteo: pd.DataFrame) -> pd.DataFrame:
        """Merge energy and meteorological information on ``DateTime``."""
        return pd.merge(df_energy, df_meteo, on="DateTime", how="left")

    @staticmethod
    def melt_energy(df: pd.DataFrame) -> pd.DataFrame:
        """Convert wide energy dataframe to long format per plant/metric."""
        value_vars = [c for c in df.columns if c.startswith("UP")]
        id_vars = [c for c in df.columns if c not in value_vars]
        return Transformer.melt_to_long(df, id_vars=id_vars, value_vars=value_vars)

    @staticmethod
    def melt_pvsyst(df: pd.DataFrame) -> pd.DataFrame:
        """Melt combined PVSyst dataframe."""
        id_vars = [c for c in ["Date", "Plant"] if c in df.columns]
        value_vars = [c for c in df.columns if c not in id_vars]
        return df.melt(id_vars=id_vars, value_vars=value_vars, var_name="Metric", value_name="Value")


# Alias para mantener compatibilidad con el código existente
DataTransformer = Transformer


def standardize_datetime(df, date_col='Date', time_col='Time', datetime_col='DateTime') -> pd.DataFrame:
    """Wrapper para compatibilidad con los tests."""
    return DataTransformer.standardize_datetime(df, date_col=date_col, time_col=time_col, datetime_col=datetime_col)


def expand_datetime(df, datetime_col='DateTime', up_to='minute') -> pd.DataFrame:
    """Wrapper para compatibilidad con los tests."""
    return DataTransformer.expand_datetime(df, datetime_col=datetime_col, up_to=up_to)
