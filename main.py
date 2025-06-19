import os
from etl.pipeline import ETLPipeline

# Puedes usar dotenv si decides guardar las rutas en un .env
# from dotenv import load_dotenv
# load_dotenv()

# Definir rutas de archivos (puedes mover esto a un JSON o .env en el futuro)
file_paths = {
    "cms": os.path.join("data", "CMS_Mejorado.xlsx"),
    "energia": os.path.join("data", "Energia.xlsx"),
    "meteo": os.path.join("data", "Meteo.xlsx"),
    "up1_pvsyst": os.path.join("data", "UP1_pvsyst.xlsx"),
    "up2_pvsyst": os.path.join("data", "UP2_pvsyst.xlsx"),
    "up3_pvsyst": os.path.join("data", "UP3_pvsyst.xlsx"),
    "up4_pvsyst": os.path.join("data", "UP4_pvsyst.xlsx"),
}

def main():
    print("🚀 Iniciando pipeline ETL para Power BI...")
    pipeline = ETLPipeline(file_paths)
    pipeline.run()

if __name__ == "__main__":
    main()
