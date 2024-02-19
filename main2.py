import streamlit as st
import mysql.connector
import random
import hashlib
from mysql.connector import Error

# Function to create SQL connection
def create_database_connection(host, database, username, password):
    if not host or not database or not username or not password:
        st.error("Please provide all the required connection details.")
        return None
    try:
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=username,
            password=password
        )
        if connection.is_connected():
            db_info = connection.get_server_info()
            st.success(f"Connected to MySQL Server version {db_info}")
            return connection
    except Error as e:
        st.error(f"Error while connecting to MySQL: {e}")
        return None

# Function to fetch data from selected columns
def fetch_column_data(connection, table_name, selected_columns):
    cursor = connection.cursor()
    selected_columns_str = ', '.join(selected_columns)
    cursor.execute(f"SELECT {selected_columns_str} FROM {table_name};")
    data = cursor.fetchall()
    return data

# Function to display selected column data with anonymization
def display_selected_column_data(connection, table_name, selected_columns, selected_user, anonymization_mode):
    column_data = fetch_column_data(connection, table_name, selected_columns)
    st.write("Original Data:")
    st.write(column_data)
    st.write("Anonymized Data:")
    anonymized_data = anonymize_data(column_data, anonymization_mode)
    st.write(anonymized_data)
    
def create_view_sql_query(connection, table_name, selected_columns, selected_user, anonymization_mode):
    view_name = f"{selected_user}_view"
    anonymized_columns = []
    for column in selected_columns:
        if anonymization_mode == "Pseudonymization":
            anonymized_columns.append(f"'User_{random.randint(1000, 9999)}' AS {column}")
        elif anonymization_mode == "Generalization":
            anonymized_columns.append("'Sensitive' AS " + column)
        elif anonymization_mode == "Randomization":
            anonymized_columns.append(f"{random.randint(0, 100)} AS {column}")
        elif anonymization_mode == "Data Masking":
            anonymized_columns.append("'XXXXX' AS " + column)
        elif anonymization_mode == "Hashing":
            anonymized_columns.append(f"SHA2({column}, 256) AS {column}")

    anonymized_columns_str = ', '.join(anonymized_columns)

    # Fetch all column names from the table
    cursor = connection.cursor()
    cursor.execute(f"DESCRIBE {table_name};")
    all_columns = cursor.fetchall()
    all_column_names = [column[0] for column in all_columns]
    
    # Remove selected columns from all column names
    remaining_columns = [column for column in all_column_names if column not in selected_columns]
    
    # Construct the SELECT query for non-anonymized columns in the original order
    remaining_columns_str = ', '.join(remaining_columns)
    
    # Construct the SELECT query
    select_query = f"SELECT {remaining_columns_str}, {anonymized_columns_str}"
    
    # Construct the final SELECT query
    select_query += f" FROM {table_name} WHERE name = '{selected_user}';"
    
    # Construct the CREATE VIEW query
    create_view_query = f"CREATE OR REPLACE VIEW {view_name} AS {select_query}"
    return create_view_query

# Function to fetch MySQL users
def fetch_mysql_users(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT user FROM mysql.user;")
    users = cursor.fetchall()
    user_list = [user[0] for user in users]
    return user_list

# Function to fetch tables and create dropdown
def select_table(connection):
    cursor = connection.cursor()
    cursor.execute("SHOW TABLES;")
    tables = cursor.fetchall()
    table_names = [table[0] for table in tables]
    selected_table = st.selectbox("Select a table:", table_names)
    st.success(f"You selected table: {selected_table}")
    return selected_table

# Function to display columns of selected table
def display_columns(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"DESCRIBE {table_name};")
    columns = cursor.fetchall()
    column_names = [column[0] for column in columns]
    st.success(f"Columns of {table_name}: {', '.join(column_names)}")
    return column_names

# Function to perform data anonymization
def anonymize_data(data, anonymization_mode):
    anonymized_data = []
    for row in data:
        anonymized_row = []
        for value in row:
            if anonymization_mode == "None":
                anonymized_row.append(value)
            elif anonymization_mode == "Pseudonymization":
                anonymized_row.append(f"User_{random.randint(1000, 9999)}")
            elif anonymization_mode == "Generalization":
                anonymized_row.append("Sensitive")
            elif anonymization_mode == "Randomization":
                anonymized_row.append(random.randint(0, 100))
            elif anonymization_mode == "Data Masking":
                anonymized_row.append("XXXXX")
            elif anonymization_mode == "Hashing":
                anonymized_row.append(hashlib.sha256(str(value).encode()).hexdigest())
            else:
                anonymized_row.append(value)
        anonymized_data.append(anonymized_row)
    return anonymized_data

# Function to check if user is admin
def is_admin(connection, username):
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM mysql.user WHERE User = '{username}' AND Super_priv = 'Y';")
    result = cursor.fetchone()
    return result[0] > 0

# Function to create user-specific view
def create_user_specific_view(connection, table_name, selected_columns, selected_user, anonymization_mode):
    view_name = f"{selected_user}_view"
    anonymized_columns = []
    for column in selected_columns:
        if anonymization_mode == "Pseudonymization":
            anonymized_columns.append(f"'User_{random.randint(1000, 9999)}' AS {column}")
        elif anonymization_mode == "Generalization":
            anonymized_columns.append("'Sensitive' AS " + column)
        elif anonymization_mode == "Randomization":
            anonymized_columns.append(f"{random.randint(0, 100)} AS {column}")
        elif anonymization_mode == "Data Masking":
            anonymized_columns.append("'XXXXX' AS " + column)
        elif anonymization_mode == "Hashing":
            anonymized_columns.append(f"SHA2({column}, 256) AS {column}")

    anonymized_columns_str = ', '.join(anonymized_columns)

    # Construct the SELECT query for the user-specific data
    select_query = f"SELECT * FROM {table_name} WHERE name = '{selected_user}'"
    
    # Construct the CREATE VIEW query
    create_view_query = f"CREATE OR REPLACE VIEW {view_name} AS {select_query}"
    return create_view_query

# Function to grant access to user-specific view
def grant_access_to_user_specific_view(connection, selected_user):
    view_name = f"{selected_user}_view"
    try:
        cursor = connection.cursor()
        cursor.execute(f"GRANT SELECT ON {view_name} TO '{selected_user}'@'%';")
        connection.commit()
        st.success(f"Granted SELECT permission on {view_name} to user: {selected_user}")
    except Error as e:
        st.error(f"Error granting access to view: {e}")

# Function to set default database for user
def set_default_database_for_user(connection, selected_user, database_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"ALTER USER '{selected_user}'@'%' DEFAULT ROLE '{database_name}';")
        connection.commit()
        st.success(f"Set default database for user: {selected_user} to: {database_name}")
    except Error as e:
        st.error(f"Error setting default database for user: {e}")

# Streamlit app
def main():
    st.title("MySQL Database Connection")

    # Input fields for host, database, username, and password
    host = st.text_input("Enter MySQL Host:")
    database = st.text_input("Enter Database Name:")
    username = st.text_input("Enter Username:")
    password = st.text_input("Enter Password:", type="password")
    new_column_order = ['id', 'name', 'email', 'aadhar_card_number', 'pan_card_number']
    # Button to initiate database connection
    connect_button = st.button("Connect to MySQL Database")

    if host and database and username and password:
        connection = create_database_connection(host, database, username, password)
        if connection:
            st.success("Database connection successful.")
            # Check if user is admin or non-admin
            if is_admin(connection, username):
                # Admin functionalities
                st.write("Admin functionalities go here.")
                selected_table = select_table(connection)
                column_names = display_columns(connection, selected_table)
                selected_columns = st.multiselect("Select columns to anonymize:", column_names)

                # Fetch MySQL users
                mysql_users = fetch_mysql_users(connection)
                selected_user = st.selectbox("Select MySQL User:", mysql_users)
                if selected_columns and selected_user:
                    anonymization_modes = ["None", "Pseudonymization", "Generalization", "Randomization", "Data Masking", "Hashing"]
                    selected_anonymization_mode = st.selectbox("Select Anonymization Mode:", anonymization_modes)
                    display_selected_column_data(connection, selected_table, selected_columns, selected_user, selected_anonymization_mode)
                    
                    # Button to create VIEW
                    create_view_button = st.button("Create VIEW")
                    if create_view_button:
                        view_query = create_view_sql_query(connection, selected_table, selected_columns, selected_user, selected_anonymization_mode)
                        try:
                            cursor = connection.cursor()
                            cursor.execute(view_query)
                            connection.commit()
                            st.success(f"View created successfully. Query: {view_query}")
                            
                            # Create user-specific view
                            create_user_specific_view(connection, selected_table, selected_columns, selected_user, selected_anonymization_mode)
                            
                            # Grant access to user-specific view
                            grant_access_to_user_specific_view(connection, selected_user)
                            
                            # Set default database for user (Optional)
                            set_default_database_for_user(connection, selected_user, database)
                            
                        except Error as e:
                            st.error(f"Error creating view: {e}")

                        # Display the CREATE VIEW query
                        st.subheader("Create VIEW Query:")
                        st.code(view_query, language="sql")
            else:
                # Non-admin functionalities
                st.write("Non-admin functionalities go here.")
                # Fetch view created for the current user
                user_view = f"{username}_view"
                cursor = connection.cursor()
                cursor.execute(f"SELECT * FROM {user_view};")
                view_data = cursor.fetchall()
                
                # Fetch column names of the view
                cursor.execute(f"DESCRIBE {user_view};")
                column_names = [column[0] for column in cursor.fetchall()]
                
                st.write("View Data:")
                # Display the table with column names and data
                st.table([column_names] + view_data)

if __name__ == "__main__":
    main()
