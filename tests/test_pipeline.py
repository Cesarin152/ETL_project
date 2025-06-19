import pandas as pd
from etl.pipeline import ETLPipeline


def test_minimal_pipeline(tmp_path, monkeypatch):
    # Crear archivos CSV minimos
    cms = pd.DataFrame({"Fecha": ["2025-01-01"], "hora": ["00:00:00"]})
    energy = pd.DataFrame({
        "Date": ["2025-01-01"],
        "Time": ["00:00:00"],
        'Universidad Panamá 1 - Medidor Janitza UP1 - ACTIVE ENERGY (kWh)': [1.0],
        'Universidad Panamá 1 - Medidor Janitza UP1 - EXPORTED ACTIVE ENERGY (kWh)': [0.0],
        'Universidad Panamá 1 - Medidor Janitza UP1 - IMPORTED ACTIVE ENERGY (kWh)': [1.0],
        'Universidad Panamá 1 - Medidor Janitza UP1 - REACTIVE ENERGY (kVArh)': [3.0],
        'Universidad Panamá 2 - Medidor Janitza UP2 - ACTIVE ENERGY (kWh)': [1.0],
        'Universidad Panamá 2 - Medidor Janitza UP2 - EXPORTED ACTIVE ENERGY (kWh)': [0.0],
        'Universidad Panamá 2 - Medidor Janitza UP2 - IMPORTED ACTIVE ENERGY (kWh)': [1.0],
        'Universidad Panamá 2 - Medidor Janitza UP2 - REACTIVE ENERGY (kVArh)': [3.0],
        'Universidad Panamá 3 - Medidor Janitza UP3 - ACTIVE ENERGY (kWh)': [1.0],
        'Universidad Panamá 3 - Medidor Janitza UP3 - EXPORTED ACTIVE ENERGY (kWh)': [0.0],
        'Universidad Panamá 3 - Medidor Janitza UP3 - IMPORTED ACTIVE ENERGY (kWh)': [1.0],
        'Universidad Panamá 3 - Medidor Janitza UP3 - REACTIVE ENERGY (kVArh)': [3.0],
        'Universidad Panamá 4 - Medidor Janitza UP4 - ACTIVE ENERGY (kWh)': [1.0],
        'Universidad Panamá 4 - Medidor Janitza UP4 - EXPORTED ACTIVE ENERGY (kWh)': [0.0],
        'Universidad Panamá 4 - Medidor Janitza UP4 - IMPORTED ACTIVE ENERGY (kWh)': [1.0],
        'Universidad Panamá 4 - Medidor Janitza UP4 - REACTIVE ENERGY (kVArh)': [3.0],
    })
    meteo = pd.DataFrame({
        "Date": ["2025-01-01"],
        "Time": ["00:00:00"],
        'Universidad Panamá 1 - Meteo - Ambient Temperature (ºC)': [20],
        'Universidad Panamá 1 - Meteo - Panel Temperature (ºC)': [21],
        'Universidad Panamá 1 - Meteo - Plant Insolation (kWh/m2)': [0.1],
        'Universidad Panamá 1 - Meteo - Plant Irradiance (W/m2)': [1000],
        'Universidad Panamá 1 - Meteo - Relative Humidity (%)': [50],
        'Universidad Panamá 3 - Meteo - Ambient Temperature (ºC)': [20],
        'Universidad Panamá 3 - Meteo - Panel Temperature (ºC)': [21],
        'Universidad Panamá 3 - Meteo - Plant Irradiance (W/m2)': [1000],
        'Universidad Panamá 3 - Meteo - Relative Humidity (%)': [50],
    })
    pvsyst = pd.DataFrame({
        "Date": ["2025-01"],
        'GlobHor (kWh/m²)': [171],
        'DiffHor (kWh/m²)': [100],
        'T_Amb (°C)': [25],
        'GlobInc (kWh/m²)': [160],
        'GlobEff (kWh/m²)': [150],
        'EArray (GWh)': [1.2],
        'E_Grid (GWh)': [1.1],
        'PR (proporción)': [0.9],
    })

    paths = {}
    for name, df in {
        "cms": cms,
        "energia": energy,
        "meteo": meteo,
        "up1_pvsyst": pvsyst,
        "up2_pvsyst": pvsyst,
        "up3_pvsyst": pvsyst,
        "up4_pvsyst": pvsyst,
    }.items():
        file_path = tmp_path / f"{name}.csv"
        df.to_csv(file_path, index=False)
        paths[name] = str(file_path)

    # Evitar accesos reales a la base de datos desde el pipeline
    class DummyDB:
        def insert_dataframe(self, df, table_name):
            pass

    monkeypatch.setattr('etl.pipeline.DatabaseLoader', lambda: DummyDB())

    pipeline = ETLPipeline(paths)
    pipeline.run()

    assert not pipeline.data["energia_long"].empty
    assert not pipeline.data["pvsyst_long"].empty
