from pymongo import MongoClient

# Connection details (http://localhost:27017/)
hostname = "localhost"
port = 27017  # Default MongoDB port
# Create a MongoDB Client instance:
client = MongoClient(hostname, port)

show_DBs = client.list_database_names()
print(show_DBs)

## TWO ways to CREATE DataBases:
# accessing it based in attribute
db1 = client.test_db_1
# accessing it based in dictionary
db2 = client["test_db_2"]

## Create a collection -----------------------------------------------------------------------------------------------
col = db1["collection_1"]
col.insert_one({"name": "coll1"})
print(db1.list_collection_names())
print(f'Number of collection: {len(db1.list_collection_names())}')

## Find Data -----------------------------------------------------------------------------------------------
# insert multiple records
col.insert_many([{"name": "coll2"}, {"name": "cool3"}])

# get all documents
all_documents = db1.collection_1.find()
for i in all_documents:
    print(i["name"])

# get only one record:
print(db1.collection_1.find_one())


## Count Nº of files inside collection -----------------------------------------------------------------------------------------------

print(db1.collection_1.count_documents({}))
