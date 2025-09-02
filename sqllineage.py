"""
SQL Lineage Analysis Module

This module provides functions for analyzing SQL queries and generating
source-to-target data lineage mappings in JSON format.
"""

import re
import json
from typing import Dict, List, Any, Optional
from datetime import datetime


def analyze_sql_lineage(sql_content: str) -> Dict[str, Any]:
    """
    Analyze SQL content and return source-to-target mapping in JSON format.

    Args:
        sql_content (str): The SQL query content to analyze

    Returns:
        Dict[str, Any]: JSON structure containing source-to-target mappings
    """
    try:
        # Parse the SQL to extract lineage information
        mappings = _parse_sql_lineage(sql_content)

        # Structure the response
        result = {
            "analysis_timestamp": datetime.now().isoformat(),
            "sql_content": sql_content[:500] + "..." if len(sql_content) > 500 else sql_content,
            "mappings": mappings,
            "summary": {
                "total_mappings": len(mappings),
                "source_tables": list(set(m["source_table"] for m in mappings if m.get("source_table"))),
                "target_tables": list(set(m["target_table"] for m in mappings if m.get("target_table")))
            }
        }

        return result

    except Exception as e:
        return {
            "error": f"Failed to analyze SQL lineage: {str(e)}",
            "analysis_timestamp": datetime.now().isoformat(),
            "sql_content": sql_content[:200] + "..." if len(sql_content) > 200 else sql_content
        }


def _parse_sql_lineage(sql_content: str) -> List[Dict[str, Any]]:
    """
    Parse SQL content to extract source-to-target mappings.

    This is a simplified parser that handles basic SQL patterns.
    For production use, consider integrating with more sophisticated SQL parsers.
    """
    mappings = []

    # Normalize SQL
    sql = sql_content.upper().strip()

    # Remove comments
    sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)

    # Extract table names from FROM clauses
    from_pattern = r'\bFROM\s+([`\w\.\-]+)'
    from_matches = re.findall(from_pattern, sql, re.IGNORECASE)

    # Extract table names from JOIN clauses
    join_pattern = r'\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+([`\w\.\-]+)'
    join_matches = re.findall(join_pattern, sql, re.IGNORECASE)

    # Extract table names from INSERT INTO
    insert_pattern = r'\bINSERT\s+INTO\s+([`\w\.\-]+)'
    insert_matches = re.findall(insert_pattern, sql, re.IGNORECASE)

    # Extract table names from CREATE TABLE AS
    create_pattern = r'\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`\w\.\-]+)'
    create_matches = re.findall(create_pattern, sql, re.IGNORECASE)

    # Extract column references
    select_pattern = r'\bSELECT\s+(.*?)\bFROM\b'
    select_match = re.search(select_pattern, sql, re.IGNORECASE | re.DOTALL)

    if select_match:
        select_clause = select_match.group(1)
        # Improved column extraction - handle aliases and qualified names better
        column_pattern = r'(\w+(?:\.\w+)?(?:\s+AS\s+\w+)?|\w+\.\w+|\w+)'
        raw_columns = re.findall(column_pattern, select_clause, re.IGNORECASE)

        # Clean and process columns
        columns = []
        for col in raw_columns[:20]:  # Limit to first 20 columns
            col = col.strip()
            if col and not col.upper() in ['AS', 'FROM', 'SELECT', 'WHERE', 'GROUP', 'ORDER', 'BY', 'HAVING']:
                # Handle qualified columns like table.column
                if '.' in col:
                    parts = col.split('.')
                    if len(parts) == 2:
                        source_col = parts[1]
                        target_col = source_col
                    else:
                        source_col = col
                        target_col = col
                else:
                    source_col = col
                    target_col = col

                # Handle AS aliases
                if ' AS ' in col.upper():
                    parts = col.upper().split(' AS ')
                    if len(parts) == 2:
                        source_col = parts[0].strip()
                        target_col = parts[1].strip()

                columns.append({
                    "source_column": source_col,
                    "target_column": target_col,
                    "data_type": "UNKNOWN"
                })

        # Combine all source tables
        source_tables = from_matches + join_matches

        # Determine target tables
        target_tables = insert_matches + create_matches

        # If no explicit target, try to infer from context
        if not target_tables and 'INTO' in sql:
            into_match = re.search(r'\bINTO\s+([`\w\.\-]+)', sql, re.IGNORECASE)
            if into_match:
                target_tables = [into_match.group(1)]

        # Generate mappings
        for source_table in source_tables:
            for target_table in target_tables or ['UNKNOWN_TARGET']:
                # Clean table names
                source_clean = _clean_table_name(source_table)
                target_clean = _clean_table_name(target_table)

                mapping = {
                    "source_table": source_clean,
                    "target_table": target_clean,
                    "transformation_type": _infer_transformation_type(sql),
                    "columns": columns
                }
                mappings.append(mapping)

    return mappings


def _clean_table_name(table_name: str) -> str:
    """Clean and normalize table names."""
    # Remove quotes and brackets
    cleaned = re.sub(r'[`"\[\]]', '', table_name)
    return cleaned.strip()


def _infer_transformation_type(sql: str) -> str:
    """Infer the type of transformation from SQL."""
    if 'INSERT' in sql and 'SELECT' in sql:
        return "INSERT_SELECT"
    elif 'CREATE' in sql and 'SELECT' in sql:
        return "CREATE_AS_SELECT"
    elif 'UPDATE' in sql:
        return "UPDATE"
    elif 'DELETE' in sql:
        return "DELETE"
    elif 'MERGE' in sql:
        return "MERGE"
    else:
        return "SELECT"


def get_lineage_summary(mappings: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate a summary of the lineage mappings.

    Args:
        mappings: List of source-to-target mappings

    Returns:
        Dict containing summary statistics
    """
    if not mappings:
        return {"total_mappings": 0, "source_tables": [], "target_tables": []}

    source_tables = list(set(m["source_table"] for m in mappings if m.get("source_table")))
    target_tables = list(set(m["target_table"] for m in mappings if m.get("target_table")))

    transformation_types = {}
    for mapping in mappings:
        trans_type = mapping.get("transformation_type", "UNKNOWN")
        transformation_types[trans_type] = transformation_types.get(trans_type, 0) + 1

    return {
        "total_mappings": len(mappings),
        "source_tables": source_tables,
        "target_tables": target_tables,
        "transformation_types": transformation_types,
        "total_columns": sum(len(m.get("columns", [])) for m in mappings)
    }


# Example usage
if __name__ == "__main__":
    # Test SQL
    test_sql = """
    INSERT INTO target_schema.fact_sales
    SELECT
        c.customer_id,
        p.product_name,
        o.order_date,
        o.quantity * p.price as total_amount
    FROM source_schema.customers c
    INNER JOIN source_schema.orders o ON c.customer_id = o.customer_id
    INNER JOIN source_schema.products p ON o.product_id = p.product_id
    WHERE o.order_date >= '2023-01-01'
    """

    result = analyze_sql_lineage(test_sql)
    print(json.dumps(result, indent=2))
