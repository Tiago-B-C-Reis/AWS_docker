from pymongo import MongoClient

# Connection details (http://localhost:27017/)
hostname = "localhost"
port = 27017  # Default MongoDB port
# Create a MongoDB Client instance:
client = MongoClient(hostname, port)
# -------------------------------------------------------------

# Query Data and Filter -------------------------------------------------------------
# Create DataBase by attribute based
db1 = client.test_db_1
# Create collection/table
col = db1["collection_2"]

# "Truncate" the collection/table:
col.delete_many({})

# Insert multiple records
documents = [{"name": "coll1", "id": 1},
             {"name": "coll2", "id": 2},
             {"name": "coll3", "id": 3}]
col.insert_many(documents)

# Get all documents
print("All records: ")
for result in col.find():
    print(result)

# Query to filter the results:
query = {"name": "coll1"}
for result in col.find(query):
    print(f"Result matching the name: {result}")

# query to filter the results
# and return only the id
query = {"name": "coll2"}
projection = {"id": 1}  # Only the field "id" is going to appear.
for result in col.find(query, projection):
    print(f"Id of the results that matched the name: \n {result}")


# Update Document -------------------------------------------------------------
query2 = {"id": 1}
update = {"$set": {"name": "new game"}}
col.update_one(query, update)

print("\nUpdate id:1")
for res in col.find():
    print(res)

# update more than one document
query = {}
update = {"$set": {"new column": "new value"}}
col.update_many(query, update)
print("\nUpdate all records")
for res in col.find():
    print(res)


# Delete Document -------------------------------------------------------------
query3 = {"id": 1}
col.delete_one(query)
print("\nDelete id:2 ")
for res in col.find():
    print(res)

# update more than one document:
query4 = {"$or": [{"id": 2}, {"id": 3}]}
col.delete_many(query)
print("Delete 2 records ")
for res in col.find():
    print(res)


