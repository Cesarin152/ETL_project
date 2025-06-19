import pandas as pd
import numpy as np
from scipy import stats

class DataCleaner:
    """
    Contiene métodos estáticos para limpieza de datos:
    - Nulos
    - Valores negativos
    - Outliers
    """

    @staticmethod
    def fill_missing_values(df: pd.DataFrame, method='mean', category_col=None, threshold=0.05) -> pd.DataFrame:
        """
        Rellena o elimina valores nulos según el método y un umbral definido.
        """
        df = df.copy()
        null_ratio = df.isnull().mean().max()

        if null_ratio < threshold:
            return df.dropna()

        for col in df.columns:
            if df[col].isnull().sum() == 0:
                continue

            if pd.api.types.is_numeric_dtype(df[col]):
                if category_col and category_col in df.columns:
                    try:
                        if method in ['mean', 'median']:
                            df[col] = df.groupby(category_col)[col].transform(
                                lambda x: x.fillna(x.mean() if method == 'mean' else x.median())
                            )
                        elif method in ['ffill', 'bfill']:
                            df[col] = df.groupby(category_col)[col].transform(
                                lambda x: x.fillna(method=method)
                            )
                        else:
                            raise ValueError("Método no soportado.")
                    except Exception:
                        valor = df[col].mean() if method == 'mean' else df[col].median()
                        df[col] = df[col].fillna(valor)
                else:
                    valor = df[col].mean() if method == 'mean' else df[col].median()
                    df[col] = df[col].fillna(valor)
            else:
                # Categorías: usar moda
                moda = df[col].mode().iloc[0] if not df[col].mode().empty else 'Desconocido'
                df[col] = df[col].fillna(moda)

        return df

    @staticmethod
    def fix_negatives(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """
        Convierte valores negativos en positivos en las columnas indicadas.
        """
        df = df.copy()
        for col in columns:
            if col in df.columns:
                df.loc[df[col] < 0, col] = df[col].abs()
        return df

    @staticmethod
    def remove_outliers(df: pd.DataFrame, column: str, threshold: float = 3.0) -> pd.DataFrame:
        """Elimina outliers usando el método del rango intercuartílico (IQR)."""
        df = df.copy()
        if column in df.columns and pd.api.types.is_numeric_dtype(df[column]):
            q1 = df[column].quantile(0.25)
            q3 = df[column].quantile(0.75)
            iqr = q3 - q1
            lower = q1 - threshold * iqr
            upper = q3 + threshold * iqr
            return df[(df[column] >= lower) & (df[column] <= upper)]
        return df


# Helpers para compatibilidad con tests
def fill_missing_values(df: pd.DataFrame, method: str = 'mean', category_col: str | None = None, threshold: float = 0.05) -> pd.DataFrame:
    """Wrapper que delega en :class:`DataCleaner` para rellenar nulos."""
    return DataCleaner.fill_missing_values(df, method=method, category_col=category_col, threshold=threshold)


def fill_missing(df: pd.DataFrame, method: str = 'mean', category_col: str | None = None, threshold: float = 0.05) -> pd.DataFrame:
    """Compatibilidad: alias de :func:`fill_missing_values`."""
    return DataCleaner.fill_missing_values(df, method=method, category_col=category_col, threshold=threshold)


def remove_outliers(df: pd.DataFrame, column: str, threshold: float = 3.0) -> pd.DataFrame:
    """Wrapper que delega en :class:`DataCleaner` para eliminar outliers."""
    return DataCleaner.remove_outliers(df, column=column, threshold=threshold)
