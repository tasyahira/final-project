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
        return

    # Load JSON data into a pandas DataFrame
    df = pd.DataFrame(json_data)

    # Create a SQLAlchemy engine
    engine = create_engine(f'mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_dbname}')

    # Insert DataFrame into MySQL database
    df.to_sql('covid_data', con=engine, if_exists='append', index=False)

    # Close the database connection
    engine.dispose()

if __name__ == "__main__":
    etl_src_to_stg()


# import pandas as pd
# from sqlalchemy import create_engine
# import requests

# # Define MySQL connection parameters
# mysql_username = 'digitalSkola'
# mysql_password = 'digitalSkola'
# mysql_host = 'localhost'
# mysql_port = 3307  # Change to your MySQL port if different
# mysql_dbname = 'final_project'

# # Fetch JSON data from the API
# url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian"
# params = {"level": "kab"}
# headers = {"Content-Type": "application/json"}
# response = requests.get(url, params=params, headers=headers)
# if response.status_code == 200:
#     json_data = response.json()['data']['content']
# else:
#     print("Failed to fetch data from the API")
#     json_data = None

# # Load JSON data into a pandas DataFrame
# if json_data:
#     df = pd.DataFrame(json_data)

#     # Create a SQLAlchemy engine
#     engine = create_engine(f'mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_dbname}')

#     # # Execute the DDL statement to create the table
#     # with engine.connect() as connection:
#     #     connection.execute(ddl_statement)

#     # Insert DataFrame into MySQL database
#     df.to_sql('covid_data', con=engine, if_exists='append', index=False)

#     # Close the database connection
#     engine.dispose()
# else:
#     print("No data fetched from the API.")
