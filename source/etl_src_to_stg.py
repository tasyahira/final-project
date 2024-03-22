import pandas as pd
from sqlalchemy import create_engine
import requests

def etl_src_to_stg():
    # Define MySQL connection parameters
    mysql_username = 'digitalSkola'
    mysql_password = 'digitalSkola'
    mysql_host = 'localhost'
    mysql_port = 3307  # Change to your MySQL port if different
    mysql_dbname = 'final_project'

    # Fetch JSON data from the API
    url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian"
    params = {"level": "kab"}
    headers = {"Content-Type": "application/json"}
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        json_data = response.json()['data']['content']
    else:
        print("Failed to fetch data from the API")
        json_data = None

    # Load JSON data into a pandas DataFrame
    if json_data:
        df = pd.DataFrame(json_data)

        # Create a SQLAlchemy engine
        engine = create_engine(f'mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_dbname}')

        # Insert DataFrame into MySQL database (covid_data table)
        df.to_sql('covid_data', con=engine, if_exists='append', index=False)

        # Data transformation for case_details table
        # Melt the DataFrame to combine relevant columns
        df_melted = df.melt(id_vars=['kode_prov', 'nama_prov', 'kode_kab', 'nama_kab', 'tanggal'], var_name='status_name', value_name='total')
        # Extract the 'status' and 'detail' parts from the 'status_name' column
        df_melted['status'] = df_melted['status_name'].apply(lambda x: x.split('_')[0])
        df_melted['detail'] = df_melted['status_name'].apply(lambda x: '_'.join(x.split('_')[1:]) if '_' in x else '')
        # Drop the 'status_name' column
        df_melted.drop(columns=['status_name'], inplace=True)

        # Define column data types based on DataFrame types
        dtype_mapping = {
            'kode_prov': 'VARCHAR(255)',
            'nama_prov': 'VARCHAR(255)',
            'kode_kab': 'VARCHAR(255)',
            'nama_kab': 'VARCHAR(255)',
            'tanggal': 'DATE',
            'status': 'VARCHAR(255)',
            'detail': 'VARCHAR(255)',
            'total': 'INT'
        }

        # Create case_details table
        with engine.connect() as connection:
            columns = ', '.join([f'{col} {dtype}' for col, dtype in dtype_mapping.items()])
            connection.execute(f'CREATE TABLE IF NOT EXISTS case_details ({columns})')

        # Insert DataFrame into MySQL database (case_details table)
        df_melted.to_sql('case_details', con=engine, if_exists='append', index=False)

        # Close the database connection
        engine.dispose()
    else:
        print("No data fetched from the API.")