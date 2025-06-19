# ETL Project

This project contains an ETL pipeline that processes several Excel files and loads the results into a relational database. The main entry point is `main.py`.

## Environment variables

Database credentials must be available as environment variables (or in a `.env` file):

- `DB_USER` – database user
- `DB_PASSWORD` – database password
- `DB_HOST` – database host (default: `localhost`)
- `DB_PORT` – database port (default: `5432`)
- `DB_NAME` – database name
- `DB_DRIVER` – SQLAlchemy driver (default: `postgresql`)

You may also provide a `DB_URL` variable with the full connection string if desired.

## Excel files

All input spreadsheets should be placed inside the `Data/` directory at the project root. The filenames expected by `main.py` are:

- `CMS_Mejorado.xlsx`
- `Energia.xlsx`
- `Meteo.xlsx`
- `UP1_pvsyst.xlsx`
- `UP2_pvsyst.xlsx`
- `UP3_pvsyst.xlsx`
- `UP4_pvsyst.xlsx`

## Running the pipeline

Execute the following commands from the project root:

```bash
pip install -r requirements.txt
# configure .env with the variables mentioned above
python main.py
```

The pipeline will load the Excel files, transform the data and insert the results into the configured database.
