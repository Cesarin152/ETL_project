# Diccionario de renombrado de columnas de energía
ENERGY_COLUMNS_RENAME = {
    'Universidad Panamá 1 - Medidor Janitza UP1 - ACTIVE ENERGY (kWh)': 'UP1_Act_MWh',
    'Universidad Panamá 1 - Medidor Janitza UP1 - EXPORTED ACTIVE ENERGY (kWh)': 'UP1_Exp_MWh',
    'Universidad Panamá 1 - Medidor Janitza UP1 - IMPORTED ACTIVE ENERGY (kWh)': 'UP1_Imp_MWh',
    'Universidad Panamá 1 - Medidor Janitza UP1 - REACTIVE ENERGY (kVArh)': 'UP1_Rea_MVArh',
    'Universidad Panamá 2 - Medidor Janitza UP2 - ACTIVE ENERGY (kWh)': 'UP2_Act_MWh',
    'Universidad Panamá 2 - Medidor Janitza UP2 - EXPORTED ACTIVE ENERGY (kWh)': 'UP2_Exp_MWh',
    'Universidad Panamá 2 - Medidor Janitza UP2 - IMPORTED ACTIVE ENERGY (kWh)': 'UP2_Imp_MWh',
    'Universidad Panamá 2 - Medidor Janitza UP2 - REACTIVE ENERGY (kVArh)': 'UP2_Rea_MVArh',
    'Universidad Panamá 3 - Medidor Janitza UP3 - ACTIVE ENERGY (kWh)': 'UP3_Act_MWh',
    'Universidad Panamá 3 - Medidor Janitza UP3 - EXPORTED ACTIVE ENERGY (kWh)': 'UP3_Exp_MWh',
    'Universidad Panamá 3 - Medidor Janitza UP3 - IMPORTED ACTIVE ENERGY (kWh)': 'UP3_Imp_MWh',
    'Universidad Panamá 3 - Medidor Janitza UP3 - REACTIVE ENERGY (kVArh)': 'UP3_Rea_MVArh',
    'Universidad Panamá 4 - Medidor Janitza UP4 - ACTIVE ENERGY (kWh)': 'UP4_Act_MWh',
    'Universidad Panamá 4 - Medidor Janitza UP4 - EXPORTED ACTIVE ENERGY (kWh)': 'UP4_Exp_MWh',
    'Universidad Panamá 4 - Medidor Janitza UP4 - IMPORTED ACTIVE ENERGY (kWh)': 'UP4_Imp_MWh',
    'Universidad Panamá 4 - Medidor Janitza UP4 - REACTIVE ENERGY (kVArh)': 'UP4_Rea_MVArh',
}

# Diccionario de renombrado de columnas de PVSyst
PVSYST_COLUMNS_RENAME = {
    "GlobHor (kWh/m²)": "GlobHor_kWh_m2",
    'DiffHor (kWh/m²)': "DiffHor_kWh_m2",
    "T_Amb (°C)": "T_Amb_C",
    'GlobInc (kWh/m²)': "GlobInc_kWh_m2",
    'GlobEff (kWh/m²)': "GlobEff_kWh_m2",
    'EArray (GWh)': "EArray_GWh",
    'E_Grid (GWh)': "E_Grid_GWh",
    'PR (proporción)': "PR_proporcion",
}

# Diccionario de renombrado de columnas meteorológicas
METEO_COLUMNS_RENAME = {
    'Universidad Panamá 1 - Meteo - Ambient Temperature (ºC)': 'UP1_Tamb_C',
    'Universidad Panamá 1 - Meteo - Panel Temperature (ºC)': 'UP1_Tpan_C',
    'Universidad Panamá 1 - Meteo - Plant Insolation (kWh/m2)': 'UP1_Ins_kWh_m2',
    'Universidad Panamá 1 - Meteo - Plant Irradiance (W/m2)': 'UP1_Irr_W_m2',
    'Universidad Panamá 1 - Meteo - Relative Humidity (%)': 'UP1_Humid_pct',
    'Universidad Panamá 3 - Meteo - Ambient Temperature (ºC)': 'UP3_Tamb_C',
    'Universidad Panamá 3 - Meteo - Panel Temperature (ºC)': 'UP3_Tpan_C',
    'Universidad Panamá 3 - Meteo - Plant Irradiance (W/m2)': 'UP3_Irr_W_m2',
    'Universidad Panamá 3 - Meteo - Relative Humidity (%)': 'UP3_Humid_pct',
    # 'Universidad Panamá 3 - Meteo - Plant Insolation (kWh/m2)': 'UP3_Ins_kWh_m2',  # calculado manualmente
}

# Mapeo general para acceso unificado
rename_dicts = {
    "energia": ENERGY_COLUMNS_RENAME,
    "meteo": METEO_COLUMNS_RENAME,
    "pvsyst": PVSYST_COLUMNS_RENAME,
}
