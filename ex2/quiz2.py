import mysql.connector
import pandas as pd
from sqlalchemy import create_engine, text

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    database="astronomy_db"
)
cursor = mydb.cursor()
engine = create_engine("mysql+mysqlconnector://root@localhost:3306/astro_database")

#1.
query = """
SELECT name, Diam_pc, dist_iso
FROM star_clusters
ORDER BY Diam_pc DESC
LIMIT 5;
"""
cursor.execute(query)
rows = cursor.fetchall()
df = pd.DataFrame(rows, columns=["name", "Diam_pc", "dist_iso"])
print("ex:1 ")
print(df)

#2.
query = """
SELECT AVG(pmRA) AS avg_pmRA, AVG(pmDE) AS avg_pmDE
FROM star_clusters
WHERE Plx > 1;
"""
cursor.execute(query)
rows = cursor.fetchall()
df = pd.DataFrame(rows, columns=["avg_pmRA", "avg_pmDE"])
print("ex:2 ")
print(df)

#3. 
query = """
SELECT name, dist_iso, dist_PLX, ABS(dist_iso - dist_PLX) AS delta_dist
FROM star_clusters
WHERE ABS(dist_iso - dist_PLX) > 500;
"""
cursor.execute(query)
rows = cursor.fetchall()
df = pd.DataFrame(rows, columns=["name", "dist_iso", "dist_PLX", "delta_dist"])
print("ex3: ")
print(df)

#4.
query = """
SELECT AVG(FeH) AS avg_FeH
FROM star_clusters
WHERE age > 2;
"""
cursor.execute(query)
rows = cursor.fetchall()
df = pd.DataFrame(rows, columns=["avg_FeH"])
print("ex4: ")
print(df)

#5.
query = """
SELECT name, Plx, sigPM, e_Plx
FROM star_clusters
WHERE sigPM < 0.5 AND e_Plx < 0.2;
"""
cursor.execute(query)
rows = cursor.fetchall()
df = pd.DataFrame(rows, columns=["name", "Plx", "sigPM", "e_Plx"])
print("ex5: ")
print(df)

#6. 
def query_cluster_by_name(cluster_name):
    query = text("SELECT * FROM star_clusters WHERE name = :name")
    with engine.connect() as conn:
        result = conn.execute(query, {"name": cluster_name})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

print("ex6: Information for cluster with my initial 'M' 'Mamajek_1':")
print(query_cluster_by_name("Mamajek_1"))

cursor.close()
mydb.close()
