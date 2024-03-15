import mysql.connector
import pandas as pd

# MySQL connection parameters
mysql_host = 'localhost'
mysql_port = '3307'
mysql_user = 'digitalSkola'
mysql_password = 'digitalSkola'
mysql_database = 'final_project'

# Connect to MySQL database
mysql_conn = mysql.connector.connect(
    host=mysql_host,
    port=mysql_port,
    user=mysql_user,
    password=mysql_password,
    database=mysql_database
)

# Execute SQL query and read results into a DataFrame
sql_query = "SELECT * FROM covid_data"
df = pd.read_sql_query(sql_query, mysql_conn)

# Close MySQL connection
mysql_conn.close()

# Melt the DataFrame to bring status details into rows
melted_df = pd.melt(df, id_vars=['kode_prov', 'kode_kab', 'tanggal'],
                    var_name='status_name', value_name='status_value')

# Extract status details from status_name
melted_df['status_detail'] = melted_df['status_name'].str.split('_', expand=True)[1]

# Filter out rows with status names
melted_df = melted_df[melted_df['status_detail'].notna()]

# Reorder columns
final_df = melted_df[['status_name', 'status_detail', 'kode_prov', 'kode_kab', 'tanggal', 'status_value']]

print(final_df)
