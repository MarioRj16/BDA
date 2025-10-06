import pandas as pd
import sys
from pymongo import MongoClient
import json

# Exercise 1 


# Exercise 1 
# Convert dias_catalogue.csv to a structured JSON file

# read the dataset into a pandas dataframe
df = pd.read_csv('dias_catalogue.csv')

# Create Nested dict (Object)
df['position'] = df[['RA_ICRS', 'DE_ICRS', 'Plx', 'dist_PLX']].apply(
    lambda s: s.to_dict(), axis=1
)

df['features'] = df[['r50', 'Vr', 'age', 'FeH', 'Diam_pc']].apply(
    lambda s: s.to_dict(), axis=1
)

# Write out Name and features to a json file
df[['name', 'position', 'features']].to_json(
    "dias_catalogue_filtered.json", 
    orient="records", 
    date_format="iso", 
    double_precision=10, 
    force_ascii=True, 
    date_unit="ms", 
    default_handler=None, 
    indent=2
)

### Exercise 2 - Connect to a Database

#Create a mongoDB connection using python (pyMongo library). 
#Create a collection called star_database
#Use pip install pymongo if you don't have the pymongo library installed.
client = MongoClient("mongodb://localhost:27017/")
db = client["star_database"]
collection = db["star_database"]

print("MongoDB connection successful!")
print("Using database:", db.name)
print("Using collection:", collection.name)


### Exercise 3 - Insert documents
#Insert at all star records (using the JSON file) into the collection using insert_many().
#Verify the total number of documents using:

#``` print(collection.count_documents({}))  ```
result = collection.delete_many({})

# 3️⃣ Load the JSON data created in Exercise 1
with open("dias_catalogue_filtered.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# 4️⃣ Insert all documents into the collection
if isinstance(data, list):  # make sure JSON is a list of dicts
    collection.insert_many(data)
else:
    print("JSON format not as expected — should be a list of records.")

# 5️⃣ Verify insertion by counting total documents
print("Total documents in collection:", collection.count_documents({}))


### Exercise 4 - Find all documents

#Write a query using find() to retrieve all the documents in the collection, and print them out (one per line). Use a loop like:

#```
#for doc in collection.find():
#    print(doc)
#```
for doc in collection.find():
    print(doc)


### Exercise 5 - Query with filters
#Write queries to:

#* Find stars with features.age > 5.

#* Find stars with position.dist_PLX < 100.

#* Find stars with features.FeH < -0.5.

#* Combine conditions: age > 7 and dist_PLX < 100.

print("Stars with features.age > 5:")
for doc in collection.find({"features.age": {"$gt": 5}}):
    print(doc)

print("\nStars with position.dist_PLX < 100:")
for doc in collection.find({"position.dist_PLX": {"$lt": 100}}):
    print(doc)

print("\nStars with features.FeH < -0.5:")
for doc in collection.find({"features.FeH": {"$lt": -0.5}}):
    print(doc)

print("\nStars with features.age > 7 and position.dist_PLX < 100:")
for doc in collection.find({"$and": [{"features.age": {"$gt": 7}}, {"position.dist_PLX": {"$lt": 100}}]}):
    print(doc)


### Exercise 6 - Sort and limit results

#Find the 3 oldest stars (highest features.age).

#Find the 5 nearest stars (smallest position.dist_PLX).

#Use .sort(...).limit(...).

print("3 oldest stars:")
for doc in collection.find().sort("features.age", -1).limit(3):
    print(doc)

print("\n5 nearest stars:")
for doc in collection.find().sort("position.dist_PLX", 1).limit(5):
    print(doc)

### Exercise 7 - Update documents

#Multiply all features.Diam_pc values by 2 using $mul.

#Add a new field features.luminosity = 1.0 for stars with features.FeH > 0 using $set.

#Check that updates worked.

print("Before updates:")
for doc in collection.find().limit(5):  # print first 5 documents to verify
    print(doc)
collection.update_many({}, {"$mul": {"features.Diam_pc": 2}})
collection.update_many({"features.FeH": {"$gt": 0}}, {"$set": {"features.luminosity": 1.0}})
print("After updates:")
for doc in collection.find().limit(5):  # print first 5 documents to verify
    print(doc)

### Exercise 8 - Delete documents

#Delete all stars where position.dist_PLX > 1000.

#Delete one star by its name.

#Print how many documents remain.

delete_result = collection.delete_many({"position.dist_PLX": {"$gt": 1000}})
print(f"Deleted {delete_result.deleted_count} documents where position.dist_PLX > 1000.")
delete_result = collection.delete_one({"name": "SomeStarName"})  # replace with an actual name
print(f"Deleted {delete_result.deleted_count} document with name 'SomeStarName'.")
print("Total documents remaining:", collection.count_documents({}))


### Exercise 9 - Aggregation — statistics

#Use the aggregation pipeline to:

#Compute the average age of all stars.

#Group stars into metallicity bins:

#* FeH < -1

#* -1 ≤ FeH ≤ 0

#* FeH > 0
#Count how many stars in each bin.

#Find the maximum and minimum position.dist_PLX

avg_age = collection.aggregate([
    {"$group": {"_id": None, "avgAge": {"$avg": "$features.age"}}}
])
for result in avg_age:
    print("Average age of all stars:", result["avgAge"])

metallicity_bins = collection.aggregate([
    {"$bucket": {
        "groupBy": "$features.FeH",
        "boundaries": [-float("inf"), -1, 0, float("inf")],
        "default": "Other",
        "output": {"count": {"$sum": 1}}
    }}
])
for bin in metallicity_bins:
    print(f"Metallicity bin: {bin['_id']}, Count: {bin['count']}")

dist_stats = collection.aggregate([
    {"$group": {"_id": None, "maxDist": {"$max": "$position.dist_PLX"}, "minDist": {"$min": "$position.dist_PLX"}}}
])
for result in dist_stats:
    print("Max position.dist_PLX:", result["maxDist"], "Min position.dist_PLX:", result["minDist"])

### Exercise 10 - Aggregation with computed fields

#Use $project to create new fields:

#* features.radius_pc = features.Diam_pc / 2.

#* position.dist_ly = position.dist_PLX × 3.26 (convert parsecs to light-years).

#* Show only name, features.Diam_pc, features.radius_pc, and position.dist_ly.

projected_fields = collection.aggregate([
    {"$project": {
        "name": 1,
        "features.Diam_pc": 1,
        "features.radius_pc": {"$divide": ["$features.Diam_pc", 2]},
        "position.dist_ly": {"$multiply": ["$position.dist_PLX", 3.26]}
    }}
])
for doc in projected_fields:
    print(doc)


### Exercise 11 - Projection and nested fields

#Write queries to:

#* Show only name, position.RA_ICRS, and position.DE_ICRS.

#* Show all fields except _id.

#* Show the full features object but exclude position.plx.

print("Only name, position.RA_ICRS, and position.DE_ICRS:")
for doc in collection.find({}, {"name": 1, "position.RA_ICRS": 1, "position.DE_ICRS": 1, "_id": 0}):
    print(doc)

print("All fields except _id:")
for doc in collection.find({}, {"_id": 0}):
    print(doc)

print("Full features object but exclude position.plx:") 
for doc in collection.find({}, {"position.Plx": 0, "_id": 0}):
    print(doc)