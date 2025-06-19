from abc import ABC, abstractmethod
import pandas as pd

class DataSource(ABC):
    """
    Clase abstracta base para todas las fuentes de datos.
    """

    @abstractmethod
    def load(self) -> pd.DataFrame:
        """
        Método para cargar y retornar los datos como un DataFrame.
        Debe ser implementado por todas las subclases.
        """
        pass
