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

# Create a collection
col = db["users"]

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
# col.insert_many(documents)

# List all Collections inside ´test_db_1´ Database:
print(db.list_collection_names())




# 1 - Users that live in New York:
query1 = {"city": "New York"}
projection1 = {"name": 1, "city": 1}  # Only the field "id" is going to appear.
for result in col.find(query1, projection1):
    print(f"\n Id of the results that matched the name: \n {result}")
    
    
    

# 2 - Get the users that have as hobby painting:
query2 = {"hobbies": "painting"}
projection2 = {"name": 1, "hobbies": 1}  # Only the field "id" is going to appear.
for result in col.find(query2, projection2):
    print(f"\n Id of the results that matched the name: \n {result}")
    
    
    

# 3 - Calculate the number of users:
print("Number of users: ", col.count_documents({}))



# 4 - Calculate the number of users that have more than 30 years, group by city:
pipeline2 = [
    {
        "$match": {
            "age": {"$gt": 30}  # Filter documents where age is greater than 30
        }
    },
    {
        "$group": {
            "_id": "$city",  # Group by city
            "count": {"$sum": 1}  # Count documents in each group
        }
    }
]

result_2 = list(col.aggregate(pipeline2))

if result_2:
    print("\nNumber of users that have more than 30 years: ")
    for item in result_2:
        print(f"City: {item['_id']}, Count: {item['count']}")
else:
    print("\nNo users found.")
    



# 5 - Calculate the number of users that have as hobby "fitness" grouped by city:
pipeline3 = [
    {
        "$match": {
            "hobbies": "fitness"  # Filter documents where hobby is "fitness"
        }
    },
    {
        "$group": {
            "_id": "$city",  # Group by city
            "count": {"$sum": 1}  # Count documents in each group
        }
    }
]

result_3 = list(col.aggregate(pipeline3))

if result_3:
    print("\nNumber of users that have 'fitness' as a hobby:")
    for item in result_3:
        print(f"City: {item['_id']}, Count: {item['count']}")
else:
    print("\nNo users found with 'fitness' as a hobby.")




# 6 - Calculate the number of users that have less than 20 years old grouped by city:
pipeline4 = [
    {
        "$match": {
            "age": {"$lt": 20}  # Filter documents where age is less than 20
        }
    },
    {
        "$group": {
            "_id": "$city",  # Group by city
            "count": {"$sum": 1}  # Count documents in each group
        }
    }
]

result_4 = list(col.aggregate(pipeline4))

if result_4:
    print("\nNumber of users that are less than 20 years old:")
    for item in result_4:
        print(f"City: {item['_id']}, Count: {item['count']}")
else:
    print("\nNo users found that are less than 20 years old.")