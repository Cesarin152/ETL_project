import pandas as pd
import os
from etl.sources.base_source import DataSource


class FileSource(DataSource):
    """Carga archivos locales (`.csv` o `.xlsx`)."""

    def __init__(self) -> None:
        pass

    @staticmethod
    def _load_file(path: str) -> pd.DataFrame:
        if path.endswith('.csv'):
            df = pd.read_csv(path)
        elif path.endswith('.xlsx'):
            df = pd.read_excel(path)
        else:
            raise ValueError("Formato no soportado. Usa .csv o .xlsx")

        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        return df

    def load_excel(self, path: str) -> pd.DataFrame:
        """Compatibilidad con el pipeline: carga el archivo indicado."""
        if not os.path.exists(path):
            raise FileNotFoundError(f"Archivo no encontrado: {path}")
        return self._load_file(path)

    # Para cumplir la interfaz ``DataSource``
    def load(self, path: str) -> pd.DataFrame:  # type: ignore[override]
        return self.load_excel(path)
