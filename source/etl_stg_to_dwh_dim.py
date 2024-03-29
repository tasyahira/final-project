import mysql.connector
import psycopg2

def etl_stg_to_dwh_dim():
    # MySQL connection parameters
    mysql_host = 'localhost'
    mysql_port = '3307'
    mysql_user = 'digitalSkola'
    mysql_password = 'digitalSkola'
    mysql_database = 'final_project'

    # PostgreSQL connection parameters
    postgresql_host = 'localhost'
    postgresql_port = '5433'
    postgresql_user = 'digitalSkola'
    postgresql_password = 'digitalSkola'
    postgresql_database = 'final_project_postgres'

    try:
        # Connect to MySQL database
        mysql_conn = mysql.connector.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
        
        # MySQL cursor
        mysql_cursor = mysql_conn.cursor()

        # Define 5 MySQL queries
        mysql_queries = [
            "select distinct kode_prov, nama_prov from covid_data;",
            "select distinct kode_kab, kode_prov, nama_kab from covid_data;",
            "select ROW_NUMBER() OVER(ORDER BY status,detail), kode_prov, nama_prov, kode_kab, nama_kab, tanggal, status, detail, total from case_details where detail in ('dikarantina', 'discarded', 'meninggal', 'diisolasi');",
            # "SELECT * FROM table4;",
            # "SELECT * FROM table5;"
        ]

        # Connect to PostgreSQL database
        postgresql_conn = psycopg2.connect(
            host=postgresql_host,
            port=postgresql_port,
            user=postgresql_user,
            password=postgresql_password,
            database=postgresql_database
        )

        # PostgreSQL cursor
        postgresql_cursor = postgresql_conn.cursor()

        # Define 5 PostgreSQL INSERT queries
        postgresql_queries = [
            "INSERT INTO dim_province (province_id, province_name) VALUES (%s, %s);",
            "INSERT INTO dim_district (district_id, province_id, district_name) VALUES (%s, %s, %s);",
            "INSERT INTO case_details (id, province_id, province_name, district_id, district_name, case_date, status_name, status_detail, total) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
            # "INSERT INTO table4 (column1, column2) VALUES (%s, %s);",
            # "INSERT INTO table5 (column1, column2) VALUES (%s, %s);"
        ]

        # Execute MySQL queries and insert data into PostgreSQL tables
        for mysql_query, postgresql_query in zip(mysql_queries, postgresql_queries):
            # Execute MySQL query
            mysql_cursor.execute(mysql_query)
            mysql_data = mysql_cursor.fetchall()

            # Insert data into PostgreSQL table
            postgresql_cursor.executemany(postgresql_query, mysql_data)
        
        # Commit the transaction
        postgresql_conn.commit()

    except Exception as e:
        print("Error:", e)
        # Rollback the transaction in case of error
        postgresql_conn.rollback()

    finally:
        # Close connections
        if mysql_conn:
            mysql_cursor.close()
            mysql_conn.close()
        if postgresql_conn:
            postgresql_cursor.close()
            postgresql_conn.close()