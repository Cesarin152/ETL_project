import os
from dotenv import load_dotenv

# Cargar variables desde el archivo .env
load_dotenv()

# Rutas a archivos Excel
ENERGY_FILE = os.getenv("ENERGY_FILE")
METEO_FILE = os.getenv("METEO_FILE")
CMS_FILE = os.getenv("CMS_FILE")
UP1_PVSYST_FILE = os.getenv("UP1_PVSYST_FILE")
UP2_PVSYST_FILE = os.getenv("UP2_PVSYST_FILE")
UP3_PVSYST_FILE = os.getenv("UP3_PVSYST_FILE")
UP4_PVSYST_FILE = os.getenv("UP4_PVSYST_FILE")

# Base de datos
DB_URL = os.getenv("DB_URL")

# API (futuro)
API_URL = os.getenv("API_URL")
API_TOKEN = os.getenv("API_TOKEN")
