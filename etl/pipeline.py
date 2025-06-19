from etl.sources.file_source import FileSource
from etl.transformer import DataTransformer
from etl.cleaner import DataCleaner
from etl.db_loader import DatabaseLoader
from etl.mappings import rename_dicts

class ETLPipeline:
    def __init__(self, file_paths: dict[str, str]):
        """
        Inicializa el pipeline con rutas de archivos.

        Args:
            file_paths (dict): Diccionario con nombre lógico -> ruta archivo.
        """
        self.file_paths = file_paths
        self.source = FileSource()
        self.transformer = DataTransformer()
        self.cleaner = DataCleaner()
        self.db = DatabaseLoader()

        # DataFrames cargados
        self.data = {}

    def run(self):
        """
        Ejecuta el pipeline completo de ETL.
        """
        print("▶️ Iniciando proceso ETL...")

        # 1. Cargar archivos
        for name, path in self.file_paths.items():
            print(f"📂 Cargando: {name}")
            self.data[name] = self.source.load_excel(path)

        # 2. Estandarizar fechas
        self.data["cms"] = self.transformer.standardize_datetime(
            self.data["cms"], date_col="Fecha", time_col="hora", datetime_col="DateTime"
        )
        self.data["energia"] = self.transformer.standardize_datetime(
            self.data["energia"], date_col="Date", time_col="Time", datetime_col="DateTime"
        )
        self.data["meteo"] = self.transformer.standardize_datetime(
            self.data["meteo"], date_col="Date", time_col="Time", datetime_col="DateTime"
        )

        # 3. Expandir fechas
        for key in ["cms", "energia", "meteo"]:
            self.data[key] = self.transformer.expand_datetime(self.data[key], "DateTime", up_to="minute")

        # 4. Renombrar columnas
        self.data["energia"].rename(columns=rename_dicts["energia"], inplace=True)
        self.data["meteo"].rename(columns=rename_dicts["meteo"], inplace=True)
        for i in range(1, 5):
            key = f"up{i}_pvsyst"
            self.data[key].rename(columns=rename_dicts["pvsyst"], inplace=True)

        # 5. Unir PVSyst
        self.data["pvsyst"] = self.transformer.combine_pvsyst([
            self.data["up1_pvsyst"],
            self.data["up2_pvsyst"],
            self.data["up3_pvsyst"],
            self.data["up4_pvsyst"]
        ])

        # 6. Generar claves
        self.data = self.transformer.generate_keys(self.data)

        # 7. Conversión de unidades
        self.data["energia"] = self.transformer.convert_units(self.data["energia"])

        # 8. Enriquecer y unir energía con meteo
        self.data["energia_consolidada"] = self.transformer.merge_energy_meteo(
            self.data["energia"], self.data["meteo"]
        )

        # 9. Limpiar datos
        self.data["energia_consolidada"] = self.cleaner.fill_missing(
            self.data["energia_consolidada"], method='mean', category_col="key_month"
        )
        self.data["energia_consolidada"] = self.cleaner.fix_negatives(
            self.data["energia_consolidada"],
            columns=[col for col in self.data["energia_consolidada"].columns if "Imp" in col or "Exp" in col]
        )
        for col in ["UP1_Act_MWh", "UP2_Act_MWh", "UP3_Act_MWh", "UP4_Act_MWh"]:
            self.data["energia_consolidada"] = self.cleaner.remove_outliers(
                self.data["energia_consolidada"], column=col
            )

        # 10. Transformar a formato largo
        self.data["energia_long"] = self.transformer.melt_energy(self.data["energia_consolidada"])
        self.data["pvsyst_long"] = self.transformer.melt_pvsyst(self.data["pvsyst"])

        # 11. Cargar a la base de datos
        self.db.insert_dataframe(self.data["energia_long"], table_name="energia_consolidada")
        self.db.insert_dataframe(self.data["pvsyst_long"], table_name="pvsyst_datos")

        print("✅ ETL finalizado correctamente.")
