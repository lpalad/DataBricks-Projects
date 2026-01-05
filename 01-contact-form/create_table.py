import os
from databricks import sql

# Read credentials from environment variables
host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")
http_path = os.getenv("DATABRICKS_HTTP_PATH")

# Validate that all credentials are set
if not host or not token or not http_path:
    print("❌ Error: Missing Databricks credentials!")
    print("Make sure to set these environment variables:")
    print("  - DATABRICKS_HOST")
    print("  - DATABRICKS_TOKEN")
    print("  - DATABRICKS_HTTP_PATH")
    exit(1)

print("🔗 Connecting to Databricks...")

try:
    # Connect to Databricks
    # Remove https:// prefix if present
    server_hostname = host.replace("https://", "").replace("http://", "")

    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=token
    ) as connection:
        print("✓ Connected to Databricks!")

        cursor = connection.cursor()

        # SQL command to create the contacts table (without DEFAULT for compatibility)
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS default.contacts (
            id BIGINT GENERATED ALWAYS AS IDENTITY,
            full_name STRING NOT NULL,
            email STRING NOT NULL,
            mobile STRING,
            message STRING NOT NULL,
            created_at TIMESTAMP
        )
        """

        print("📝 Creating contacts table...")
        cursor.execute(create_table_sql)

        # Enable column defaults feature and add default value
        print("📝 Enabling column defaults feature...")
        cursor.execute("""
            ALTER TABLE default.contacts
            SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
        """)

        cursor.execute("""
            ALTER TABLE default.contacts
            ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP
        """)
        cursor.close()

        print("✓ Table 'contacts' created successfully!")
        print("\nTable schema:")
        print("  - id (auto-increment)")
        print("  - full_name (required)")
        print("  - email (required)")
        print("  - mobile (optional)")
        print("  - message (required)")
        print("  - created_at (auto-timestamp)")

except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

print("\n✅ Setup complete! Ready for Phase 3.")
