import pandas as pd
import requests
from etl.sources.base_source import DataSource


class APISource(DataSource):
    """
    Fuente de datos desde una API REST que retorna JSON estructurado.
    """

    def __init__(self, url: str, headers: dict = None, params: dict = None):
        self.url = url
        self.headers = headers or {}
        self.params = params or {}

    def load(self) -> pd.DataFrame:
        """
        Hace una solicitud GET a la API y convierte la respuesta en un DataFrame.

        Retorna:
        pd.DataFrame: Datos convertidos desde JSON
        """
        response = requests.get(self.url, headers=self.headers, params=self.params)

        if response.status_code != 200:
            raise ConnectionError(f"Error en API: {response.status_code} - {response.text}")

        data = response.json()

        # Intentar convertir automáticamente en DataFrame
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            # si tiene una key principal con los datos, ej: {'results': [...]} o similar
            for key in data:
                if isinstance(data[key], list):
                    return pd.DataFrame(data[key])
            return pd.DataFrame([data])  # fallback: dict simple
        else:
            raise ValueError("Formato de respuesta no compatible con pandas.DataFrame")
