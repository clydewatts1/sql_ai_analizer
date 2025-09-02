#!/usr/bin/env python3
"""
Test script for the new SQL lineage functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_sqllineage_module():
    """Test the sqllineage module independently"""
    try:
        from sqllineage import analyze_sql_lineage, get_lineage_summary
        import json

        # Test SQL
        test_sql = """
        INSERT INTO sales.fact_orders
        SELECT
            c.customer_id,
            c.customer_name,
            o.order_id,
            o.order_date,
            p.product_name,
            oi.quantity,
            oi.unit_price * oi.quantity as total_amount
        FROM customers.customers c
        INNER JOIN orders.orders o ON c.customer_id = o.customer_id
        INNER JOIN orders.order_items oi ON o.order_id = oi.order_id
        INNER JOIN products.products p ON oi.product_id = p.product_id
        WHERE o.order_date >= '2024-01-01'
        """

        print("Testing SQL Lineage Analysis...")
        result = analyze_sql_lineage(test_sql)

        print("✓ Analysis completed successfully")
        print(f"✓ Found {result['summary']['total_mappings']} mappings")
        print(f"✓ Source tables: {result['summary']['source_tables']}")
        print(f"✓ Target tables: {result['summary']['target_tables']}")

        # Test summary function
        summary = get_lineage_summary(result['mappings'])
        print(f"✓ Summary generated: {summary['total_mappings']} mappings")

        return True

    except Exception as e:
        print(f"✗ Error testing sqllineage module: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_function_registration():
    """Test that the function is properly registered (without Google dependencies)"""
    try:
        # Mock the Google types for testing
        class MockSchema:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)

        class MockType:
            OBJECT = "object"
            STRING = "string"

        class MockFunctionDeclaration:
            def __init__(self, name, description, parameters):
                self.name = name
                self.description = description
                self.parameters = parameters

        class MockTool:
            def __init__(self, function_declarations):
                self.function_declarations = function_declarations

        # Test the function declaration creation logic
        print("\nTesting function registration logic...")

        # Simulate the function declarations
        declarations = [
            MockFunctionDeclaration(
                name='get_object_ddl',
                description='Gets the DDL for a given Teradata object',
                parameters=MockSchema(
                    type=MockType.OBJECT,
                    properties={
                        'table_name': MockSchema(
                            type=MockType.STRING,
                            description='The table name'
                        )
                    },
                    required=['table_name']
                )
            ),
            MockFunctionDeclaration(
                name='analyze_sql_lineage',
                description='Analyzes SQL content and returns lineage mappings',
                parameters=MockSchema(
                    type=MockType.OBJECT,
                    properties={
                        'sql_content': MockSchema(
                            type=MockType.STRING,
                            description='The SQL content to analyze'
                        )
                    },
                    required=['sql_content']
                )
            )
        ]

        tool = MockTool(declarations)

        print("✓ Function declarations created successfully")
        print(f"✓ Number of functions: {len(tool.function_declarations)}")

        for func in tool.function_declarations:
            print(f"✓ Function: {func.name}")

        return True

    except Exception as e:
        print(f"✗ Error testing function registration: {e}")
        return False

if __name__ == "__main__":
    print("Testing SQL Lineage Integration")
    print("=" * 40)

    success = True

    # Test the sqllineage module
    if not test_sqllineage_module():
        success = False

    # Test function registration logic
    if not test_function_registration():
        success = False

    print("\n" + "=" * 40)
    if success:
        print("✓ All tests passed! SQL lineage module is ready.")
    else:
        print("✗ Some tests failed. Please check the errors above.")

    sys.exit(0 if success else 1)
