#!/usr/bin/env python3
"""
Atlan Discovery Script

Helps discover Snowflake connections, assets, and test custom metadata updates
"""

import os
import sys

try:
    from pyatlan.client.atlan import AtlanClient
    from pyatlan.model.assets import Connection, Database, Schema, Table, Column
    from pyatlan.errors import NotFoundError
except ImportError:
    print("ERROR: pyatlan not installed. Run: pip install pyatlan")
    sys.exit(1)


def main():
    # Get credentials
    api_key = os.environ.get('ATLAN_API_KEY')
    base_url = os.environ.get('ATLAN_BASE_URL')

    if not api_key or not base_url:
        print("ERROR: Set ATLAN_API_KEY and ATLAN_BASE_URL environment variables")
        sys.exit(1)

    print(f"Connecting to: {base_url}\n")

    try:
        client = AtlanClient(api_key=api_key, base_url=base_url)
        print("✓ Client initialized successfully\n")

        # Step 1: Find Snowflake connections
        print("=" * 80)
        print("STEP 1: Finding Snowflake Connections")
        print("=" * 80)

        try:
            # Use FluentSearch to find connections
            from pyatlan.model.fluent_search import FluentSearch

            results = (
                FluentSearch()
                .where(FluentSearch.asset_type(Connection))
                .where(FluentSearch.active_assets())
                .page_size(50)
                .execute(client)
            )

            snowflake_connections = []
            for conn in results:
                if isinstance(conn, Connection):
                    print(f"  Connection: {conn.name}")
                    print(f"    Type: {conn.connector_name}")
                    print(f"    Qualified Name: {conn.qualified_name}")
                    if 'snowflake' in conn.connector_name.lower():
                        snowflake_connections.append(conn)
                        print(f"    >> SNOWFLAKE CONNECTION FOUND!")
                    print()

            if not snowflake_connections:
                print("⚠ No Snowflake connections found")
                return

            sf_conn = snowflake_connections[0]
            print(f"\n✓ Using Snowflake connection: {sf_conn.name}")
            print(f"  Qualified Name: {sf_conn.qualified_name}\n")

        except Exception as e:
            print(f"✗ Error searching connections: {e}")
            import traceback
            traceback.print_exc()
            return

        # Step 2: Find a sample column asset
        print("=" * 80)
        print("STEP 2: Finding Sample Column Assets")
        print("=" * 80)

        try:
            # Search for columns from WIDE_WORLD_IMPORTERS database
            results = (
                FluentSearch()
                .where(FluentSearch.asset_type(Column))
                .where(FluentSearch.active_assets())
                .where(FluentSearch.match("databaseName", "WIDE_WORLD_IMPORTERS"))
                .page_size(10)
                .execute(client)
            )

            sample_columns = []
            for col in results:
                if isinstance(col, Column):
                    print(f"  Column: {col.name}")
                    print(f"    Qualified Name: {col.qualified_name}")
                    print(f"    Database: {col.database_name}")
                    print(f"    Schema: {col.schema_name}")
                    print(f"    Table: {col.table_name}")
                    sample_columns.append(col)
                    print()

                    if len(sample_columns) >= 3:
                        break

            if not sample_columns:
                print("⚠ No columns found in WIDE_WORLD_IMPORTERS database")
                print("\nTry searching for any column:")
                results = (
                    FluentSearch()
                    .where(FluentSearch.asset_type(Column))
                    .where(FluentSearch.active_assets())
                    .page_size(5)
                    .execute(client)
                )

                for col in results:
                    if isinstance(col, Column):
                        print(f"  Column: {col.name}")
                        print(f"    Qualified Name: {col.qualified_name}")
                        sample_columns.append(col)
                        break

        except Exception as e:
            print(f"✗ Error searching columns: {e}")
            import traceback
            traceback.print_exc()

        # Step 3: Show how to construct qualified names
        print("\n" + "=" * 80)
        print("STEP 3: Qualified Name Pattern")
        print("=" * 80)

        if sample_columns:
            sample = sample_columns[0]
            print(f"\nSample qualified name format:")
            print(f"  {sample.qualified_name}")
            print(f"\nPattern breakdown:")
            parts = sample.qualified_name.split('/')
            for i, part in enumerate(parts):
                labels = ['connector', 'type', 'account', 'database', 'schema', 'table', 'column']
                label = labels[i] if i < len(labels) else f'part{i}'
                print(f"    [{i}] {label}: {part}")

        print("\n" + "=" * 80)
        print("DISCOVERY COMPLETE")
        print("=" * 80)
        print("\nNext steps:")
        print("  1. Update dq_enrichment_v2.py with the correct qualified name pattern")
        print("  2. Run the enrichment script with your CSV file")

    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
