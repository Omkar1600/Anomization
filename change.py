import streamlit as st
import mysql.connector
import psycopg2

def connect_to_mysql(connection_url):
    try:
        conn = mysql.connector.connect(
            host=connection_url['host'],
            port=connection_url['port'],
            user=connection_url['user'],
            password=connection_url['password'],
            database=connection_url['database']
        )
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        cursor.close()
        return conn, tables, None
    except mysql.connector.Error as e:
        return None, [], f"Error connecting to MySQL database: {e}"

def connect_to_postgres(connection_url):
    try:
        conn = psycopg2.connect(
            host=connection_url['host'],
            port=connection_url['port'],
            user=connection_url['user'],
            password=connection_url['password'],
            database=connection_url['database']
        )
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        tables = cursor.fetchall()
        cursor.close()
        return conn, tables, None
    except psycopg2.Error as e:
        return None, [], f"Error connecting to PostgreSQL database: {e}"

def main():
    st.title("Database Connection")

    # Input fields for connection parameters
    connection_type = st.radio("Select Database Type", ("MySQL", "PostgreSQL"))
    host = st.text_input("Host", "localhost")
    port = st.number_input("Port", value=3306 if connection_type == "MySQL" else 5432, step=1)
    user = st.text_input("Username")
    password = st.text_input("Password", type="password")
    database = st.text_input("Database Name")

    connection_url = {
        'host': host,
        'port': port,
        'user': user,
        'password': password,
        'database': database
    }

    if st.button("Connect"):
        if connection_type == "MySQL":
            conn, tables, error = connect_to_mysql(connection_url)
        elif connection_type == "PostgreSQL":
            conn, tables, error = connect_to_postgres(connection_url)
        
        if error:
            st.error(error)
        elif not tables:
            st.warning("No tables found in the selected database.")
        else:
            st.success("Connected to database successfully!")

            # Dropdown menu to select table
            selected_table = st.selectbox("Select Table", tables)
            st.write("You selected:", selected_table)

if __name__ == "__main__":
    main()
