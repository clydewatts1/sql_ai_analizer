import teradatasql
import logging

# This will be updated by the main application
db_connection_params = {}

def update_db_connection_params(params):
    """Update the database connection parameters"""
    global db_connection_params
    db_connection_params.update(params)
    logging.info(f"Updated DB connection params: host={params.get('host', 'None')}")

def get_object_ddl(table_name: str) -> str:
    """Gets the DDL (Data Definition Language) for a given Teradata object (table or view).

    Use this function to discover the schema, columns, and types of any object you encounter in the user's SQL.

    Args:
        table_name: The fully-qualified name of the table or view (e.g., 'DATABASE.TABLE_NAME').

    Returns:
        The DDL string for the object, or an error message if not found or an issue occurs.
    """
    logging.info(f"[Tool] Called get_object_ddl for: {table_name}")
    if not db_connection_params.get("host"):
        return "Error: Database host not configured."

    try:
        with teradatasql.connect(**db_connection_params) as connect:
            with connect.cursor() as cur:
                try:
                    parts = table_name.split('.')
                    if len(parts) != 2:
                        return f"Error: '{table_name}' is not a fully-qualified name."
                    db_name, tbl_name = parts[0].strip(), parts[1].strip()

                    cur.execute("SELECT TableKind FROM DBC.TablesV WHERE DataBaseName = ? AND TableName = ?", (db_name, tbl_name))
                    result = cur.fetchone()
                    if not result:
                        return f"Error: Object '{table_name}' not found in DBC.TablesV."
                    
                    table_kind = result[0].strip()
                    show_command = ""
                    if table_kind in ('T', 'O'):
                        show_command = f"SHOW TABLE {table_name};"
                    elif table_kind == 'V':
                        show_command = f"SHOW VIEW {table_name};"
                    else:
                        return f"Error: Unsupported object type '{table_kind}' for {table_name}."

                    cur.execute(show_command)
                    ddl = cur.fetchone()[0]
                    return f"-- Schema for {table_name}\n{ddl}"
                except Exception as e:
                    logging.error(f"[Tool] Error fetching DDL for {table_name}: {e}", exc_info=True)
                    return f"Error fetching DDL for {table_name}: {e}"
    except Exception as e:
        logging.error(f"[Tool] Error connecting to Teradata: {e}", exc_info=True)
        return f"Error connecting to Teradata: {e}"