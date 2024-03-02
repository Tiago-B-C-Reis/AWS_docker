import pymongo
from pymongo import MongoClient

# Connection details (http://localhost:27017/)
hostname = "localhost"
port = 27017  # Default MongoDB port
# Create a MongoDB Client instance:
client = MongoClient(hostname, port)

# show databases
show_DBs = client.list_database_names()
print(show_DBs)

# set "DemoUserDataSet" database to variable "db1"
db1 = client.DemoUserDataSet
# print all collection from "DemoUserDataSet" Database
print(db1.list_collection_names())

# Load collection "users" to variable "col" (because this collection already existed)
col = db1["users"]


# Create Compound Index:
index = [("city", pymongo.TEXT), ("hobbies", pymongo.TEXT)]
col.create_index(index)
# Print the users returned by the search on the following text ("New York", "painting")
query1 = {"$text": {"$search": "New York"}}
query2 = {"$text": {"$search": "painting"}}
print(col.find(query1).explain()['queryPlanner']['winningPlan'])


