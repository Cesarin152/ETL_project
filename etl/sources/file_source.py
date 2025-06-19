import pandas as pd
import os
from etl.sources.base_source import DataSource


class FileSource(DataSource):
    """
    Implementación concreta de DataSource para cargar datos desde archivos locales.
    Soporta archivos .csv y .xlsx.
    """

    def __init__(self, filepath: str):
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Archivo no encontrado: {filepath}")
        self.filepath = filepath

    def load(self) -> pd.DataFrame:
        """
        Carga el archivo y elimina columnas innecesarias (tipo 'Unnamed').

        Retorna:
        pd.DataFrame: DataFrame limpio
        """
        if self.filepath.endswith('.csv'):
            df = pd.read_csv(self.filepath)
        elif self.filepath.endswith('.xlsx'):
            df = pd.read_excel(self.filepath)
        else:
            raise ValueError("Formato no soportado. Usa .csv o .xlsx")

        # Eliminar columnas tipo 'Unnamed'
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        return df
