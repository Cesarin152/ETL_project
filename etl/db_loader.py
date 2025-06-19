from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

class DatabaseLoader:
    def __init__(self):
        """
        Inicializa la conexión a la base de datos usando variables de entorno.
        """
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        db = os.getenv("DB_NAME")
        driver = os.getenv("DB_DRIVER", "postgresql")  # por defecto PostgreSQL

        self.engine = create_engine(f"{driver}://{user}:{password}@{host}:{port}/{db}")
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists="append"):
        """
        Inserta un DataFrame a la base de datos.

        Args:
            df (pd.DataFrame): Datos a insertar
            table_name (str): Nombre de la tabla destino
            if_exists (str): "fail", "replace", o "append"
        """
        try:
            df.to_sql(table_name, self.engine, index=False, if_exists=if_exists, method="multi")
            print(f"✅ Datos insertados en la tabla '{table_name}' ({len(df)} filas).")
        except Exception as e:
            print(f"❌ Error al insertar en la tabla '{table_name}': {e}")

    def test_connection(self):
        """
        Testea la conexión a la base de datos.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            print("✅ Conexión a base de datos exitosa.")
        except Exception as e:
            print(f"❌ Error de conexión a base de datos: {e}")
