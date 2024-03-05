import pymongo
from pymongo import MongoClient

# Connection details (http://localhost:27017/)
hostname = "localhost"
port = 27017  # Default MongoDB port
# Create a MongoDB Client instance:
client = MongoClient(hostname, port)
# -------------------------------------------------------------

# Create DataBase by attribute based:
db = client.test_db_1
# Create collection/table:
scores_col = db["scores"]
cars = db["cars"]

# "Truncate" the collection/table:
scores_col.delete_many({})
cars.delete_many({})


# Insert multiple records:
data1 = [
    {"_id": "1", "scores_spring": [8, 6], "scores_fall": 9},
    {"_id": "2", "scores_spring": [6], "scores_fall": [5, 7]}
    ]
scores_col.insert_many(data1)


# Insert multiple records:
data2 = [
    {"_id": "1", "brand": "Toyota", "model": "Corolla", "year": 2000},
    {"_id": "2", "brand": "Toyota", "model": "Yaris", "year": 2001},
    {"_id": "3", "brand": "Honda", "model": "Accord", "year": 2002}
]
cars.insert_many(data2)


## Index Types ----------------------------------------------------------
# Multikey Index.
# With without index:
query = {"scores_spring": [6]}
print(scores_col.find(query).explain()['queryPlanner']['winningPlan'])

# With index:
index = [("scores_spring", pymongo.ASCENDING)]
scores_col.create_index(index)
query1 = {"scores_spring": [6]}
print(scores_col.find(query1).explain()['queryPlanner']['winningPlan'])


# Text Indexes.
index = [("brand", pymongo.TEXT)]
cars.create_index(index)

print(f"\n Index: ", [index["name"] for index in cars.list_indexes()])

# search by a text field
# to use the $text option we need to have a text index
query2 = {"$text": {"$search": "Toyota"}}
print(cars.find(query2).explain()['queryPlanner']['winningPlan'])
