# Part I
# 1. Connect to the MongoDb instance;
# 2. Insert the example dataset to a collection named `users`, in a database named with `DemoUserDataSet`;
# 3. List all the collections in the DB `DemoUserDataSet`, and count them;
# 4. Count the number of documents in the collection `users`
# 5. Print all the documents in `users` collection
from pymongo import MongoClient

############ 1. Connect to the MongoDb instance
#  connection details
hostname = 'localhost'
port = 27017  # Default MongoDB port
# Create a MongoClient instance
client = MongoClient(hostname, port)

##################### 2. Insert the example dataset to a collection named `users`, in a database named with `DemoUserDataSet`;
# creates the db if not exist
demo_db = client.demouserdataset
# creates the collection if not exist
# if the collection exist conencts to the collection
users = demo_db['users']
users.drop()

# sample dataset
data = [
    { "name": "Alice", "age": 28, "city": "New York", "hobbies": ["reading", "gaming", "travel"] },
    { "name": "Bob", "age": 34, "city": "San Francisco", "hobbies": ["painting", "fitness"] },
    { "name": "Charlie", "age": 23, "city": "New York", "hobbies": ["coding", "music", "gaming"] },
    { "name": "Diana", "age": 31, "city": "Boston", "hobbies": ["reading", "yoga", "photography", "travel"] },
    { "name": "Ethan", "age": 27, "city": "San Francisco", "hobbies": ["fitness", "travel", "photography"] },
    { "name": "Fiona", "age": 21, "city": "Boston", "hobbies": ["reading", "gaming", "music"] },
    { "name": "John", "age": 40, "city": "Boston", "hobbies": ["reading", "travel"] },
    { "name": "James", "age": 35 },
    { "name": "Mary", "age": 28 }
]
# insert data into users collection
users.insert_many(data)

##################### 3. List all the collections in the DB `DemoUserDataSet`, and count them;
# print all the collections in demousersdataset
print(f"Collections on database {demo_db.name}: {demo_db.list_collection_names()}")
# number of collections in demousersdataset
print(f"Number of collections on database {demo_db.name}: {len(demo_db.list_collection_names())}")

##################### 4. Count the number of documents in the collection `users`
total_users = users.count_documents({})
# count the number of documents in collection users
print(f"Number of documents in users: {total_users}")

##################### 5. Print all the documents in `users` collection
# print all documents in users collection
all_users = users.find()
for user in all_users:
    print(user)


##########################################################################
# Part II
# 6. Print the name of all users that live in Boston;
# 7. Print the name of all users that have more than 30 years;
# 8. Update all users that live in Boston, change the city to Chicago;
# 9. Delete one user that lives in San Francisco;
    
################### Clean the collection and insert the data again
# clean the collection
users.drop()

# sample dataset
data = [
    { "name": "Alice", "age": 28, "city": "New York", "hobbies": ["reading", "gaming", "travel"] },
    { "name": "Bob", "age": 34, "city": "San Francisco", "hobbies": ["painting", "fitness"] },
    { "name": "Charlie", "age": 23, "city": "New York", "hobbies": ["coding", "music", "gaming"] },
    { "name": "Diana", "age": 31, "city": "Boston", "hobbies": ["reading", "yoga", "photography", "travel"] },
    { "name": "Ethan", "age": 27, "city": "San Francisco", "hobbies": ["fitness", "travel", "photography"] },
    { "name": "Fiona", "age": 21, "city": "Boston", "hobbies": ["reading", "gaming", "music"] },
    { "name": "John", "age": 40, "city": "Boston", "hobbies": ["reading", "travel"] },
    { "name": "James", "age": 35 },
    { "name": "Mary", "age": 28 }
]
# insert data into users collection
users.insert_many(data)

###################### 6. Print the name of all users that live in Boston;
query = {"city": "Boston"}
projection={"_id":0,"name":1}
for user in users.find(query, projection):
    print(f"User that live in Boston: {user}")

## or
query = {"city": "Boston"}
for user in users.find(query):
    print(f"User that live in Boston: {user['name']}")

#################### 7. Print the name of all users that have more than 30 years;
query = {"age": {"$gte":30}}
projection={"_id":0,"name":1}
for user in users.find(query, projection):
    print(f"User that has more than 30 years old: {user}")

#################### 8. Update all users that live in Boston, change the city to Chicago;
query_to_filter = {"city":"Boston"}
update = {"$set": {"city": "Chicago"}}
users.update_many(query_to_filter, update)
for user in users.find():
    print(user)


################# 9. Delete one user that lives in San Francisco;
query_to_filter = {"city":"San Francisco"}
users.delete_one(query_to_filter)
print(f"Number of documents after deleting user in San Francisco: {users.count_documents({})}")  

##########################################################################
# Part III
# 10. Create an index on users collection with:
# - field: city, type: text
# - field: hobbies, type: text
# 11. Print all the users returned by the search on the following text:
# - "New York"
# - "Painting"
# 12. Is it using the index? (check using the explain)

################### Clean the collection and insert the data again
# clean the collection
users.drop()

# sample dataset
data = [
    { "name": "Alice", "age": 28, "city": "New York", "hobbies": ["reading", "gaming", "travel"] },
    { "name": "Bob", "age": 34, "city": "San Francisco", "hobbies": ["painting", "fitness"] },
    { "name": "Charlie", "age": 23, "city": "New York", "hobbies": ["coding", "music", "gaming"] },
    { "name": "Diana", "age": 31, "city": "Boston", "hobbies": ["reading", "yoga", "photography", "travel"] },
    { "name": "Ethan", "age": 27, "city": "San Francisco", "hobbies": ["fitness", "travel", "photography"] },
    { "name": "Fiona", "age": 21, "city": "Boston", "hobbies": ["reading", "gaming", "music"] },
    { "name": "John", "age": 40, "city": "Boston", "hobbies": ["reading", "travel"] },
    { "name": "James", "age": 35 },
    { "name": "Mary", "age": 28 }
]
# insert data into users collection
users.insert_many(data)

###################### 10. Create an index on users collection with:
########################### - field: city, type: text
########################### - field: hobbies, type: text
import pymongo
index = [("city", pymongo.TEXT), ("hobbies",pymongo.TEXT)]
users.create_index(index)

######################## 11. Print all the users returned by the search on the following text:
############################# - "New York"
############################# - "painting"
query = {"$text" : {"$search": "New York"}}
print(f"Users:", [user["name"] for user in users.find(query)])

query = {"$text" : {"$search": "painting"}}
print(f"Users:", [user["name"] for user in users.find(query)])

############################ 12. Is it using the index? (check using the explain)
print(f"Explain query: {users.find(query).explain()['queryPlanner']}")



# Part IV
# 13. Get the users that live in New York;
# 14. Get the users that have as hobby painting;
# 15. Calculate the number of users;
# 16. Calculate the number of users that have more than 30 years, grouped by city;
# 17. Calculate the number of users that as hobby fitness grouped by city;
# 18. Calculate the number of users that have less than 25 years old grouped by city;

################### Clean the collection and insert the data again
# clean the collection
users.drop()

# sample dataset
data = [
    { "name": "Alice", "age": 28, "city": "New York", "hobbies": ["reading", "gaming", "travel"] },
    { "name": "Bob", "age": 34, "city": "San Francisco", "hobbies": ["painting", "fitness"] },
    { "name": "Charlie", "age": 23, "city": "New York", "hobbies": ["coding", "music", "gaming"] },
    { "name": "Diana", "age": 31, "city": "Boston", "hobbies": ["reading", "yoga", "photography", "travel"] },
    { "name": "Ethan", "age": 27, "city": "San Francisco", "hobbies": ["fitness", "travel", "photography"] },
    { "name": "Fiona", "age": 21, "city": "Boston", "hobbies": ["reading", "gaming", "music"] },
    { "name": "John", "age": 40, "city": "Boston", "hobbies": ["reading", "travel"] },
    { "name": "James", "age": 35 },
    { "name": "Mary", "age": 28 }
]
# insert data into users collection
users.insert_many(data)

##################### 13. Get the users that live in New York;
# Get the users that live in New York;
query = {"city": "New York"}
for user in users.find(query,projection):
    print(f"User that live in New York: {user}")

##################### 14. Get the users that have as hobby painting;
# Get the users that have as hobby painting;
query = {"hobbies": "painting"}
for user in users.find(query):
    print(f"User that have hobbies paiting: {user}")  

##################### 15. Calculate the number of users;
# Calculate the number of users;
print("Number of users: ", users.count_documents({}))  

##################### 16. Calculate the number of users that have more than 30 years, grouped by city;
# Calculate the number of users that have more than 30 years, grouped by city;
query = {"age": {"$gte":30}}
print("Number of users with more than 30 years old: ", users.count_documents(query))  

# grouped by city
pipeline_age = [
    # stage 1
    {
         '$match': {
            'age': {"$gte":30}
        }
    },
    # stage 2
    {# count
        '$group': {
            "_id" : "$city", 
            "number_users": { '$sum': 1}
        }
    }
]
for us in users.aggregate(pipeline_age):
    print("Number of users that have more than 30 years grouped by city: ",us)

############################ 17. Calculate the number of users that as hobby fitness grouped by city;
# Calculate the number of users that as hobby fitness grouped by city;
pipeline_hobby = [
    # stage 1
    {
        # add a new column with the total price
         '$match': {
            'hobbies': "fitness"
        }
    },
    # stage 2
    {
        # count
        '$group': {
            "_id" : "$city", 
            "number_users": { '$sum': 1}
        }
    }
]
for us in users.aggregate(pipeline_hobby):
    print("Number of users with hobby fitness by city: ",us)

###################### 18. Calculate the number of users that have less than 25 years old grouped by city;
# Calculate the number of users that have less than 25 years old grouped by city;
pipeline_user_age = [
    # stage 1
    {
        # add a new column with the total price
         '$match': {
            'age': {"$lt":25}
        }
    },
    # stage 2
    {
        # count
        '$group': {
            "_id" : "$city", 
            "number_users": { '$sum': 1}
        }
    }
]
for us in users.aggregate(pipeline_user_age):
    print("Number of user that have less than 25 grouped by city: ",us)

