# SOP: Connecting to the PostgreSQL Data Source in Power BI

## Purpose
This document outlines the steps required to connect to the DHIS2 PostgreSQL database using Power BI.

## Steps

### 1. Open Power BI and Get Data
- Launch Power BI Desktop.
- Click on **Home > Get Data**.
- In the window that appears, click on **More...**.

### 2. Select PostgreSQL Database
- In the **Get Data** window, go to the **Database** category.
- Select **PostgreSQL database**, then click **Connect**.

### 3. Enter Connection Details
- **Host**: `10.100.11.42:5433`  
- **Database**: `dhis2_gov`

> 📌 **Note**: You will need a username and password.  
> 👉 **Contact Fumbani** to obtain the database password.

### 4. Navigate and Select Tables
- Once connected, Power BI will load the list of available tables.
- You'll notice that some tables appear in pairs (e.g., with and without a `_resolved` suffix).
- **Select the table with the `_resolved` suffix**.  
  The `_resolved` version has column names translated to readable names instead of raw IDs.

### 5. Proceed with Transformations and Visuals
- Click **Load** or **Transform Data** to open Power Query Editor.
- Perform your custom data transformations and build your visualizations as needed.

---
