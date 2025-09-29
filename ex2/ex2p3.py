import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np

# ------------------------------
# Step 1: Load and clean CSV
# ------------------------------
df = pd.read_csv("dias_catalogue.csv")

# Replace empty strings with None and infinities with NaN
df = df.replace('', None)
df.replace([np.inf, -np.inf], np.nan, inplace=True)

# ------------------------------
# Step 2: Connect to MySQL
# ------------------------------
# If root has no password, just use "root@localhost"
engine = create_engine("mysql+mysqlconnector://root@localhost:3306/")

# Create database if it doesn't exist
with engine.connect() as conn:
    conn.execute(text("CREATE DATABASE IF NOT EXISTS astro_database"))
    conn.commit()

# Connect to the new database
engine = create_engine("mysql+mysqlconnector://root@localhost:3306/astro_database")

# ------------------------------
# Step 3: Create table
# ------------------------------
create_table_query = """
CREATE TABLE IF NOT EXISTS star_clusters (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    RA_ICRS FLOAT,
    DE_ICRS FLOAT,
    r50 FLOAT,
    r50_arcmin FLOAT,
    rmax_arcmin FLOAT,
    diamMAX_arcmin FLOAT,
    N INT,
    pmRA FLOAT,
    e_pmRA FLOAT,
    pmDE FLOAT,
    e_pmDE FLOAT,
    Vr FLOAT,
    e_Vr FLOAT,
    Nvr INT,
    Plx FLOAT,
    e_Plx FLOAT,
    sigPM FLOAT,
    flagdispPM VARCHAR(50),
    age FLOAT,
    e_age FLOAT,
    dist_iso FLOAT,
    e_dist_iso FLOAT,
    FeH FLOAT,
    e_FeH FLOAT,
    Av FLOAT,
    e_Av FLOAT,
    AG FLOAT,
    e_AG FLOAT,
    dist_PLX FLOAT,
    e_dist_PLX FLOAT,
    Diam_pc FLOAT,
    DiamMax_pc FLOAT,
    delta_dist FLOAT,
    likelihood FLOAT,
    BIC FLOAT,
    prior_FEH FLOAT,
    prior_AV FLOAT,
    REF VARCHAR(255)
)
"""

with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS star_clusters"))
    conn.execute(text(create_table_query))
    conn.commit()

print("Database and table ready!")

# ------------------------------
# Step 4: Insert CSV into MySQL
# ------------------------------
# Replace NaN with None to allow MySQL NULLs
df = df.where(pd.notnull(df), None)

# Insert using pandas to_sql
df.to_sql('star_clusters', con=engine, if_exists='append', index=False)
print(f"{len(df)} rows inserted successfully!")

# ------------------------------
# Step 5: Optional: Test query
# ------------------------------
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM star_clusters LIMIT 5"))
    for row in result:
        print(row)
