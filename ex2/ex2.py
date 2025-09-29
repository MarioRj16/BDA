import mysql.connector
import csv

# Helper function to safely convert CSV values to float
def safe_float(value):
    try:
        f = float(value)
        if f == float('inf') or f == float('-inf') or f != f:  # catches NaN
            return 0
        return f
    except:
        return 0

# Connect to MySQL server and use astronomy_db
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    database="astronomy_db"
)

cursor = mydb.cursor()

# Drop existing table if any
cursor.execute("DROP TABLE IF EXISTS star_clusters")

# Make sure database exists and use it
cursor.execute("CREATE DATABASE IF NOT EXISTS astronomy_db")
cursor.execute("USE astronomy_db")

print("Database 'astronomy_db' ready!")

### Exercise 2 - Create Table
cursor.execute("""
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
);
""")

print("Table 'star_clusters' ready!")

### Exercise 3 - Optional: Insert One Row
# cursor.execute("""
# INSERT INTO star_clusters (name, RA_ICRS, DE_ICRS, r50, r50_arcmin, rmax_arcmin, diamMAX_arcmin, N, pmRA, REF)
# VALUES ('Cluster1', 123.45, -54.321, 1.23, 0.5, 2.0, 3.0, 100, -5.67, 'Ref1')
# """)
# mydb.commit()
# print("One row inserted successfully!")

### Exercise 4 - Insert Multiple Rows from CSV
insert_query = """
INSERT INTO star_clusters (
    name, RA_ICRS, DE_ICRS, r50, r50_arcmin, rmax_arcmin, diamMAX_arcmin,
    N, pmRA, e_pmRA, pmDE, e_pmDE, Vr, e_Vr, Nvr, Plx, e_Plx, sigPM, flagdispPM,
    age, e_age, dist_iso, e_dist_iso, FeH, e_FeH, Av, e_Av, AG, e_AG,
    dist_PLX, e_dist_PLX, Diam_pc, DiamMax_pc, delta_dist, likelihood, BIC,
    prior_FEH, prior_AV, REF
) VALUES (
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s
)
"""

with open("dias_catalogue.csv", newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    rows = []
    for row in reader:
        rows.append((
            row["name"],
            safe_float(row["RA_ICRS"]),
            safe_float(row["DE_ICRS"]),
            safe_float(row["r50"]),
            safe_float(row["r50_arcmin"]),
            safe_float(row["rmax_arcmin"]),
            safe_float(row["diamMAX_arcmin"]),
            int(row["N"] or 0),
            safe_float(row["pmRA"]),
            safe_float(row["e_pmRA"]),
            safe_float(row["pmDE"]),
            safe_float(row["e_pmDE"]),
            safe_float(row["Vr"]),
            safe_float(row["e_Vr"]),
            int(row["Nvr"] or 0),
            safe_float(row["Plx"]),
            safe_float(row["e_Plx"]),
            safe_float(row["sigPM"]),
            row["flagdispPM"],
            safe_float(row["age"]),
            safe_float(row["e_age"]),
            safe_float(row["dist_iso"]),
            safe_float(row["e_dist_iso"]),
            safe_float(row["FeH"]),
            safe_float(row["e_FeH"]),
            safe_float(row["Av"]),
            safe_float(row["e_Av"]),
            safe_float(row["AG"]),
            safe_float(row["e_AG"]),
            safe_float(row["dist_PLX"]),
            safe_float(row["e_dist_PLX"]),
            safe_float(row["Diam_pc"]),
            safe_float(row["DiamMax_pc"]),
            safe_float(row["delta_dist"]),
            safe_float(row["likelihood"]),
            safe_float(row["BIC"]),
            safe_float(row["prior_FEH"]),
            safe_float(row["prior_AV"]),
            row["REF"]
        ))

    cursor.executemany(insert_query, rows)
    mydb.commit()

print(f"{cursor.rowcount} rows inserted successfully!")

### Exercise 5 - Select All Rows
cursor.execute("SELECT * FROM star_clusters WHERE DiamMax_pc > 200")
results = cursor.fetchall()
for row in results:
    print(row)


### Exercise 6 - Select Specific Columns

#Query only the columns name, RA_ICRS, DE_ICRS, and Diam_pc where Plx > 1.

cursor.execute("SELECT name, RA_ICRS, DE_ICRS, Diam_pc FROM star_clusters WHERE Plx > 1")
results = cursor.fetchall()
for row in results:
    print(row)

### Exercise 7 - Update a Value

#Update the age of one specific star cluster (choose by name) to a new value. Print the number of affected rows.

cursor.execute("UPDATE star_clusters SET age = 500 WHERE name = 'ASCC_10'")
mydb.commit()
cursor.execute("SELECT * FROM star_clusters WHERE name = 'ASCC_10'")
results = cursor.fetchall()
for row in results:
    print(row)
print(f"{cursor.rowcount} rows updated successfully!")

### Exercise 8 - Delete a Row

#Delete a star cluster from the table where name = 'NGC 188' (or any existing entry from your dataset).
cursor.execute("DELETE FROM star_clusters WHERE name = 'NGC 188'")
mydb.commit()
print(f"{cursor.rowcount} rows deleted successfully!")

### Exercise 9 - Search with LIKE

#Find all star clusters whose name starts with "NGC". Print their name and dist_PLX.


cursor.execute("SELECT name, dist_PLX FROM star_clusters WHERE name LIKE 'NGC%'")
results = cursor.fetchall()
for row in results:
    print(row)

### Exercise 10 - Aggregate Function

#Count how many star clusters have FeH < 0 (metal-poor). Print the total number.

cursor.execute("SELECT COUNT(*) FROM star_clusters WHERE FeH < 0")
result = cursor.fetchone()
print(f"Total metal-poor star clusters: {result[0]}")

cursor.close()
mydb.close()
