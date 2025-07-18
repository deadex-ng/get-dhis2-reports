import requests
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
import pandas as pd
import re

def sanitize_table_name(name, dataset_id=None):
    name = name.lower()
    name = re.sub(r'\W+', '_', name)
    if not re.match(r'^[a-z]', name):
        name = 'ds_' + name
    if len(name) > 50:
        short_name = name[:45]
        name = f"{short_name}_{dataset_id[-6:]}" if dataset_id else short_name
    return name

class DHIS2Client:
    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.headers = {"Accept": "application/json"}

    def get(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.get(url, headers=self.headers, params=params, timeout=120)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            print(f"[HTTP ERROR] {response.status_code} while accessing {url}")
            if 500 <= response.status_code < 600:
                print("  → DHIS2 server might be temporarily down or overloaded.")
            elif response.status_code == 401:
                print("  → Unauthorized. Check your DHIS2 username/password.")
            raise http_err
        except requests.exceptions.Timeout:
            print(f"[TIMEOUT] Request to {url} timed out. DHIS2 might be offline or slow to respond.")
            print("  → Try again later or check your internet connection.")
            raise
        except requests.exceptions.ConnectionError:
            print(f"[CONNECTION ERROR] Failed to connect to {url}.")
            print("  → Check if DHIS2 is up and reachable from your network.")
            raise

class DHIS2ToPostgresDynamicTables:
    def __init__(self, dhis_client, engine, start_date, end_date, dataset_orgunit_map):
        self.client = dhis_client
        self.engine = engine
        self.metadata = MetaData()
        self.start_date = start_date
        self.end_date = end_date
        self.Session = sessionmaker(bind=engine)
        self.dataset_orgunit_map = dataset_orgunit_map

    def fetch_and_store_data_elements(self):
        print("📥 Downloading data elements metadata...")
        try:
            response = self.client.get("api/dataElements", params={"paging": "false", "fields": "id,name"})
            elements = response.get("dataElements", [])
            if elements:
                df = pd.DataFrame(elements)
                df.to_sql("dhis2_data_elements", self.engine, if_exists="replace", index=False)
                print(f"✅ Stored {len(df)} data elements in table 'dhis2_data_elements'")
            else:
                print("⚠️ No data elements found.")
        except Exception as e:
            print(f"❌ Failed to fetch data elements: {e}")

    def sync(self):
        session = self.Session()
        try:
            # First fetch and store all data elements
            self.fetch_and_store_data_elements()

            print("⏳ Fetching dataset metadata...")
            all_datasets = self.client.get("api/dataSets", params={"paging": "false", "fields": "id,name"})["dataSets"]
            dataset_lookup = {d["id"]: d["name"] for d in all_datasets if d["id"] in self.dataset_orgunit_map}

            print("⏳ Fetching organisation units...")
            all_org_units = self.client.get("api/organisationUnits", params={"paging": "false", "fields": "id,name"})["organisationUnits"]
            org_lookup = {ou["id"]: ou["name"] for ou in all_org_units}

            total_reports = len(self.dataset_orgunit_map)
            for idx, (ds_id, org_unit_ids) in enumerate(self.dataset_orgunit_map.items(), start=1):
                ds_name = dataset_lookup.get(ds_id)
                if not ds_name:
                    print(f"⚠️ Skipping dataset {ds_id} (not found in DHIS2)")
                    continue

                table_name = sanitize_table_name(f"dataset_{ds_name}", dataset_id=ds_id)
                print(f"\n🔄 {idx} of {total_reports} Processing dataset '{ds_name}' → table '{table_name}'")

                all_rows = []

                for ou_id in org_unit_ids:
                    ou_name = org_lookup.get(ou_id, "Unknown Facility")
                    print(f"  📍 Fetching org unit '{ou_name}' ({ou_id})")

                    params = {
                        "dataSet": ds_id,
                        "orgUnit": ou_id,
                        "startDate": self.start_date,
                        "endDate": self.end_date
                    }

                    try:
                        data = self.client.get("api/dataValueSets", params=params)
                        for dv in data.get("dataValues", []):
                            row = {
                                "date": dv.get("period"),
                                "facility": ou_name,
                                "report_name": ds_name,
                                "data_element_combo": f"{dv.get('dataElement')}_{dv.get('categoryOptionCombo')}",
                                "value": dv.get("value")
                            }
                            all_rows.append(row)
                    except Exception as e:
                        print(f"    ❌ Error fetching data for OU {ou_id}: {e}")

                if all_rows:
                    df = pd.DataFrame(all_rows)

                    wide_df = df.pivot_table(
                        index=["date", "facility", "report_name"],
                        columns="data_element_combo",
                        values="value",
                        aggfunc="first"
                    ).reset_index()

                    wide_df.columns.name = None
                    wide_df.columns = [str(col) for col in wide_df.columns]

                    wide_df.to_sql(table_name, self.engine, if_exists="replace", index=False)
                    print(f"  ✅ Stored {len(wide_df)} rows in table '{table_name}'")
                else:
                    print(f"  ⚠️ No data found for dataset '{ds_name}'")
        finally:
            session.close()

if __name__ == "__main__":
    BASE_URL = "https://dhis2.health.gov.mw/"
    USERNAME = "xxxxxxxx"
    PASSWORD = "xxxxxxxx"
    START_DATE = "2024-01-01"
    END_DATE = "2024-12-31"

    DB_URL = "postgresql+psycopg2://myuser:mypassword@localhost:5432/mydatabase"
    engine = create_engine(DB_URL)

    # 🟩 Define only the datasets and org units you want
    # Replace with your dataset IDs and matching org unit IDs
    #Paediatric Oncology Monthly Reporting Form
    #Mental Health Facility Report
    #Maternity Monthly Report
    # CBMNC Monthly Report
    # CMAM Stock Sheet Monthly Report
    # Cervical Cancer Control Program Monthly Report
    # Covid 19 Monthly Reporting Form
    # EPI Vaccination Performance and Disease Surveillance (NEW)
    # Exposed Child Under 24 Months-Follow Up
    # Family Planning Monthly Report
    # HIV Self-Test Distribution Monthly Report
    # HMIS 15
    # HTC Health Facility Report
    # IMCI Village Clinic Monthly Consolidation Report
    # Kangaroo Mother Care Monthly Reporting Form
    # Malaria Health Facility Report
    # Maternal and Neonatal Death Report
    # Maternity Monthly Report
    dataset_orgunit_map = {
        "zysssD93UWM": ["zw8eLbN4Znw", "EQg6N2v2TXj", "GtRLLmB1Jc6"],
        "Fdn3C7gKoju": ["Rmh4wKR794k","jBJ1nrUXKIu"],
        "B0UtGNECmZW": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],
        "P4oPxnYmYHY": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],
        "ZABjSFibfGV": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],
        "NX1lpqsalRy": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],
        "gPAyiHYXHBI": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],
        "xKmkoAZLEGU": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],
        "cCsbOg15aNB": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],
        "TZwgYAeQXxL": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],
        "IhVEF2U4zhn": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],
        "q1Es3k3sZem": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],
        "Yz1PMQk1QlF": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],                                                                        
        "hWDsGIjs16g": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],                                                                        
        "ACmZFToDqxh": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],                                                                        
        "aYZsjFwm4P9": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],                                                                        
        "U31O0OHvtuS": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],                                                                        
        "B0UtGNECmZW": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"],                                                                        
        "B0UtGNECmZW": ["pciHYsH4glX","gA0WGnhCnYt","GjNQ12Y2l0F","cfzBcWqPOoy","JKAFWLrwdji","zq5yo5iRvsL","NW5K84KJ4xp","HxziIaDjatq", "I4Vox6oteWl", "Rmh4wKR794k", "jBJ1nrUXKIu", "y3FF95NnZzl", "NFqFeBSH2Re", "EiLdri7MySb", "iVOnl6X10Ym"]
    }

    dhis_client = DHIS2Client(BASE_URL, USERNAME, PASSWORD)
    syncer = DHIS2ToPostgresDynamicTables(
        dhis_client,
        engine,
        START_DATE,
        END_DATE,
        dataset_orgunit_map
    )
    syncer.sync()
