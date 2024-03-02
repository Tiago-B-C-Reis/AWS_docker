from pymongo import MongoClient

# Connection details (http://localhost:27017/)
hostname = "localhost"
port = 27017  # Default MongoDB port
# Create a MongoDB Client instance:
client = MongoClient(hostname, port)

show_DBs = client.list_database_names()
print(show_DBs)

# Create DataBase by attribute based
db1 = client.DemoUserDataSet

# Create a collection
col = db1["users"]

# "Truncate" the collection/table:
result = col.delete_many({})
print(f"{result.deleted_count} documents deleted.")


# Insert data into "users" collection:
documents = (
[
    { "name": "Alice", "age": 28, "city": "New York", "hobbies": ["reading", "gaming", "travel"] },
    { "name": "Bob", "age": 34, "city": "San Francisco", "hobbies": ["painting", "fitness"] },
    { "name": "Charlie", "age": 23, "city": "New York", "hobbies": ["coding", "music", "gaming"] },
    { "name": "Diana", "age": 31, "city": "Boston", "hobbies": ["reading", "yoga", "photography", "travel"] },
    { "name": "Ethan", "age": 27, "city": "San Francisco", "hobbies": ["fitness", "travel", "photography"] },
    { "name": "Fiona", "age": 21, "city": "Boston", "hobbies": ["reading", "gaming", "music"] },
    { "name": "John", "age": 40, "city": "Boston", "hobbies": ["reading", "travel"] },
    { "name": "James", "age": 35 },
    { "name": "Mary", "age": 28 }
])
col.insert_many(documents)


# List all collection:
print(db1.list_collection_names())
# Count then all the Collections (tables in RDMS(SQL)):
print(f'Number of Collection/Tables: {len(db1.list_collection_names())}')


# Count then all the Documents (Rows in RDMS(SQL)):
print("Nº of Documents/Rows: ", db1.users.count_documents({}))
# Get all Documents:
all_documents = db1.users.find()
for i in all_documents:
    print(i["name"])


# Print the name of all users that live in Boston:
query1 = {"city": "Boston"}
for result in col.find(query1):
    print(f"\n Name of all users that live in Boston: \n {result}")


# Print the name and the age of all users that have more than 30 years:
query2 = {"age": {"$gt": 30}}  # Adding age condition
projection = {"name": 1, "age": 1}  # Only the fields "Name" and "age" will appear.
for result in col.find(query2, projection):
    print(f"\nName and age of users aged more than 30:\n{result}")


# Update all users that live in Boston, change the city to Chicago:
query3 = {"city": "Boston"}
update = {"$set": {"city": "Chicago"}}
col.update_many(query3, update)
print("\nUpdate all records")
for res in col.find():
    print(res)


# Delete one user that lives in San Francisco:
query4 = {"city": "San Francisco"}
col.delete_one(query4)
print("\nDelete one ")
for res in col.find():
    print(res)

