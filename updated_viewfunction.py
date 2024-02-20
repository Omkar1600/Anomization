# Button to create VIEW
create_view_button = st.button("Create VIEW")
if create_view_button:
    view_query = create_view_sql_query(connection, selected_table, selected_columns, selected_user, selected_anonymization_mode)
    try:
        cursor = connection.cursor()
        cursor.execute(view_query)
        connection.commit()
        st.success(f"View created successfully. Query: {view_query}")

        # 1. Create a new table with masked data
        masked_table_name = selected_table  # Use the same name as the default table
        create_masked_table_query = f"CREATE TABLE IF NOT EXISTS masked_data_schema.{masked_table_name} AS SELECT * FROM {selected_table};"
        cursor.execute(create_masked_table_query)
        connection.commit()
        st.success(f"Masked table '{masked_table_name}' created successfully.")

        # 2. Change default schema for non-admin users
        if not is_admin(connection, username):
            alter_user_query = f"ALTER USER '{username}'@'localhost' DEFAULT ROLE 'masked_data_schema';"
            cursor.execute(alter_user_query)
            connection.commit()
            st.success(f"Default schema changed for user '{username}' to 'masked_data_schema'.")

            # 3. Revoke privileges for non-admin users to access the default schema
            revoke_query = f"REVOKE ALL PRIVILEGES ON {database}.{selected_table} FROM '{username}'@'localhost';"
            cursor.execute(revoke_query)
            connection.commit()
            st.success(f"Privileges revoked for user '{username}' to access '{selected_table}' in default schema.")

            # 4. Grant privileges for non-admin users to access the masked table
            grant_query = f"GRANT SELECT ON masked_data_schema.{masked_table_name} TO '{username}'@'localhost';"
            cursor.execute(grant_query)
            connection.commit()
            st.success(f"Privileges granted for user '{username}' to access '{masked_table_name}' in 'masked_data_schema'.")
    except Error as e:
        st.error(f"Error creating view: {e}")
