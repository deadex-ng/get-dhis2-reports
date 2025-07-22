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

# Step 3: List of form tables to process
form_tables = [
    "adult_oncology_monthly_reporting_form",
    "cbmnc_monthly_report",
    "cervical_cancer_control_program_monthly_report",
    "cmam_stock_sheet_monthly_report",
    "covid_19_monthly_reporting_form",
    "epi_vaccination_performance_and_disease_surve_AZLEGU",
    "exposed_child_under_24_months_follow_up",
    "family_planning_monthly_report",
    "hiv_self_test_distribution_monthly_report",
    "hmis_15",
    "htc_health_facility_report",
    "imci_village_clinic_monthly_consolidation_report",
    "kangaroo_mother_care_monthly_reporting_form",
    "malaria_health_facility_report_",
    "maternal_and_neonatal_death_report",
    "maternity_monthly_report",
    "mental_health_facility_report",
    "paediatric_oncology_monthly_reporting_form"
]

# Step 4: Process each form table
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

                if de_name:
                    print(f"✅ DE {de_id} → {de_name}")
                else:
                    print(f"⚠️ DE {de_id} not found")

                if co_name:
                    print(f"✅ CO {co_id} → {co_name}")
                else:
                    print(f"⚠️ CO {co_id} not found")

                de_part = (de_name or de_id).replace(" ", "_")
                co_part = (co_name or co_id or "").replace(" ", "_")

                new_name = f"{de_part}_{co_part}"
                if len(new_name) > 63:
                    new_name = new_name[:60] + "_tr"
            else:
                new_name = col

            # Handle duplicate column names
            if new_name not in seen_names:
                seen_names.add(new_name)
                rename_map[col] = new_name
            else:
                print(f"🚫 Duplicate column: {new_name}. Using original name: {col}")
                rename_map[col] = col

        # Rename columns
        form_df = form_df.rename(columns=rename_map)

        # Save renamed table
        new_table_name = f"{table_name}_resolved"
        form_df.to_sql(new_table_name, engine, index=False, if_exists='replace')
        print(f"✅ Saved resolved table as: {new_table_name}")

        # Save rename log
        # pd.DataFrame(rename_map.items(), columns=["Original", "Renamed"]) \
        #     .to_csv(f"{new_table_name}_column_map.csv", index=False)

    except Exception as e:
        print(f"❌ Failed to process {table_name}: {e}")
