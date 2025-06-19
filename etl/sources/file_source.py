import pandas as pd
import os
from etl.sources.base_source import DataSource


class FileSource(DataSource):
    """
    Implementación concreta de DataSource para cargar datos desde archivos locales.
    Soporta archivos .csv y .xlsx.
    """

    def __init__(self):
        """Fuente de archivos local sin ruta fija."""
        pass

    def load_excel(self, filepath: str) -> pd.DataFrame:
        """Carga un archivo Excel o CSV y limpia columnas auxiliares."""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Archivo no encontrado: {filepath}")

        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        elif filepath.endswith('.xlsx') or filepath.endswith('.xls'):
            df = pd.read_excel(filepath)
        else:
            raise ValueError("Formato no soportado. Usa .csv o .xlsx")

        # Eliminar columnas tipo 'Unnamed'
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        return df
