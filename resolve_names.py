import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os

# Step 1: Database connection
DB_URL = f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
engine = create_engine(DB_URL)

# Step 2: Metadata mapping
data_elements_df = pd.read_sql("SELECT id, name FROM public.dhis2_data_elements", engine)
category_options_combo_df = pd.read_sql("SELECT id, name FROM public.dhis2_category_option_combos", engine)

de_map = data_elements_df.set_index('id')['name'].str.strip().to_dict()
co_map = category_options_combo_df.set_index('id')['name'].str.strip().to_dict()

# Step 3: Dynamically get all table names except excluded ones
excluded_tables = {"dhis2_category_option_combos", "dhis2_data_elements"}

# Fetch all table names in the public schema
table_query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE';
"""
all_tables_df = pd.read_sql(table_query, engine)
form_tables = [tbl for tbl in all_tables_df['table_name'] if tbl not in excluded_tables]

# Step 4: Process each form table
failed_tables = []

for table_name in form_tables:
    print(f"\n🔄 Processing table: {table_name}")
    try:
        form_df = pd.read_sql(f"SELECT * FROM public.{table_name}", engine)

        rename_map = {}
        seen_names = set()

        for col in form_df.columns:
            if '_' in col:
                parts = col.split('_', 1)
                de_id, co_id = parts[0], parts[1] if len(parts) > 1 else None

                de_name = de_map.get(de_id)
                co_name = co_map.get(co_id)

                de_part = (de_name or de_id).replace(" ", "_")
                co_part = (co_name or co_id or "").replace(" ", "_")

                new_name = f"{de_part}_{co_part}"

                # 🧠 Smart truncation
                for skip in [20, 30, 40]:
                    if len(new_name) > 63:
                        new_name = new_name[skip:]
                if len(new_name) > 63:
                    new_name = new_name[:63]
            else:
                new_name = col

            # Handle duplicate column names
            base_name = new_name
            suffix = 1
            while new_name in seen_names:
                new_name = f"{base_name}_{suffix}"
                suffix += 1

            seen_names.add(new_name)
            rename_map[col] = new_name

        # Rename columns
        form_df = form_df.rename(columns=rename_map)

        # Save renamed table
        new_table_name = f"{table_name}_resolved"
        form_df.to_sql(new_table_name, engine, index=False, if_exists='replace')
        print(f"✅ Saved resolved table as: {new_table_name}")

    except Exception as e:
        print(f"❌ Failed to process {table_name}: {e}")
        failed_tables.append(table_name)

# Step 5: Show all failed tables
if failed_tables:
    print("\n❗ The following tables failed to process:")
    for tbl in failed_tables:
        print(f"  - {tbl}")
else:
    print("\n🎉 All tables processed successfully!")
