import pymongo
from pymongo import MongoClient

# 1. Connect to the MongoDB instance
#  connection details
hostname = 'localhost'
port = 27017  # Default MongoDB port
# Create a MongoClient instance
client = MongoClient(hostname, port)
########################################################################## ##########################################################################





## 2. Populate a COLLECTION with a DATASET ----------------------------------------------------------------------------------------
# Create DATABASE by [Attribute Based]: (Database Name = "exam_database") (Instance name that represents the DATABASE = "db")
db_instance = client.exam_database 

# Create a Collection: (COLLECTION Name = "users_collection") (Instance name that represents the COLLECTION = "u_collection")
u_collection = db_instance["users_collection"]

## Drop Collection if necessary
u_collection.drop()

# Sample Dataset
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

## Insert data (Many) into "users_collection" collection:
u_collection.insert_many(documents)

# List all Collections inside "users_collection" Database:
print(db_instance.list_collection_names())


##################### 3. LIST and COUNT all the collections in the DB `DemoUserDataSet`, and count them;  -------------------------------------
# Print all the collections in "users_collection"
print(f"Collections on database {db_instance.name}: {db_instance.list_collection_names()}")
# Number of collections in "users_collection"
print(f"Number of collections on database {db_instance.name}: {len(db_instance.list_collection_names())}")


##################### 4. Count the number of documents in the collection "users_collection" -----------------------------------------------------
total_documents = u_collection.count_documents({})
# count the number of documents in collection "users_collection"
print(f"Number of documents in u_collection: {total_documents}")


##################### 5. Print all the documents in "users_collection" collection
print("All documents: ")
for result in u_collection.find():
    print(result)








########################################################################## ##########################################################################
# Part II
# Query + Projection:
#    - Select query 
#    - Update query 
#    - Delete query 

###################### 6. Print the name of all users that live in Boston;
query_1 = {"city": "Boston"}
projection_1 ={"_id":0,"name":1} # Select the documents to show
for result in u_collection.find(query_1, projection_1):
    print(f"User that live in Boston: {result}")
## or
query_2 = {"city": "Boston"}
for result in u_collection.find(query_2):
    print(f"User that live in Boston: {result['name']}")

#################### 7. Print the name of all users that have more than 30 years;
query_3 = {"age": {"$gte":30}}
projection_2 ={"_id":0,"name":1}
for result in u_collection.find(query_3, projection_2):
    print(f"User that has more than 30 years old: {result}")

#################### 8. Update all users that live in Boston, change the city to Chicago;
query_to_filter_1 = {"city": "Boston"}
update = {"$set": {"city": "Chicago"}}
u_collection.update_many(query_to_filter_1, update)
for result in u_collection.find():
    print(result)

################# 9. Delete one user that lives in San Francisco;
query_to_filter_2 = {"city": "San Francisco"}
u_collection.delete_one(query_to_filter_2)
print(f"Number of documents after deleting user in San Francisco: {u_collection.count_documents({})}")











########################################################################## ##########################################################################
# Part III
# 10. [Create] an INDEX and [Explanin] the results.

###################################### Clean the collection and insert the data again ##################################
u_collection.drop()
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
u_collection.insert_many(documents)
########################################################################################################################

## 10. Create an index on "users_collection" collection with:
#    - field: city, type: text
#    - field: hobbies, type: text
index = [("city", pymongo.TEXT), ("hobbies",pymongo.TEXT)]
u_collection.create_index(index)

## 11. Print all the users returned by the search on the following text:
#   - "New York"
#   - "Painting"
query_4 = {"$text" : {"$search": "New York"}}
print(f"Users:", [result["name"] for result in u_collection.find(query_4)])

query_5 = {"$text" : {"$search": "painting"}}
print(f"Users:", [result["name"] for result in u_collection.find(query_5)])

## 12. Is it using the index? (check using the [Explain])
print(f"Explain query: {u_collection.find(query_5).explain()['queryPlanner']}")










########################################################################## ##########################################################################
## Part IV
# PIPELINE quering.


## 13. Get the users that live in New York;
# Get the users that live in New York;
query_6 = {"city": "New York"}
projection_3 ={"_id":0,"name":1}
for result in u_collection.find(query_6, projection_3):
    print(f"\n Id of the results that matched the name: \n {result}")


## 14. Get the users that have a hobby of painting;
# Get the users that have a hobby of painting;
query_7 = {"hobbies": "painting"}
for result in u_collection.find(query_7):
    print(f"User that have hobbies painting: {result}")  


## 15. Calculate the number of users;
# Calculate the number of users;
print("\nNumber of users: ", u_collection.count_documents({}))  



################### 16. Calculate the number of users that have more than 30 years, grouped by city;
pipeline1 = [
    # stage 1
    {
        "$match": {
            "age": {"$gt": 30}  # Filter documents where age is greater than 30
        }
    },
    # stage 2
    {# count
        "$group": {
            "_id": "$city",  # Group by city
            "count": {"$sum": 1}  # Count documents in each group
        }
    }
]

# Use the [Aggregate] function with the pipeline to instantiate the query result in "aggregation_1"
aggregation_1 = list(u_collection.aggregate(pipeline1))

 # Print the results:
if aggregation_1:
    print("\nNumber of users that have more than 30 years: ")
    for item in aggregation_1:
        print(f"City: {item['_id']}, Count: {item['count']}")
else:
    print("\nNo users found.")



################### 17. Calculate the number of users that as hobby fitness grouped by city;
pipeline2 = [
    # stage 1
    {
        "$match": {
            "hobbies": "fitness"  # Filter documents where hobby is "fitness"
        }
    },
    # stage 2
    {
        # count
        "$group": {
            "_id": "$city",  # Group by city
            "count": {"$sum": 1}  # Count documents in each group
        }
    }
]

# Use the [Aggregate] function with the pipeline to instantiate the query result in "aggregation_1"
aggregation_2 = list(u_collection.aggregate(pipeline2))

 # Print the results:
if aggregation_2:
    print("\nNumber of users that have 'fitness' as a hobby:")
    for item in aggregation_2:
        print(f"City: {item['_id']}, Count: {item['count']}")
else:
    print("\nNo users found with 'fitness' as a hobby.")



##################### 18. Calculate the number of users that are less than 25 years old grouped by city;
pipeline3 = [
    # stage 1
    {
        # Add a new column with the total price
        "$match": {
            "age": {"$lt": 20}  # Filter documents where age is less than 20
        }
    },
    # stage 2
    {
        # count
        "$group": {
            "_id": "$city",  # Group by city
            "count": {"$sum": 1}  # Count documents in each group
        }
    }
]

# Use the [Aggregate] function with the pipeline to instantiate the query result in "aggregation_3"
aggregation_3 = list(u_collection.aggregate(pipeline3))

 # Print the results:
if aggregation_3:
    print("\nNumber of users that are less than 20 years old:")
    for item in aggregation_3:
        print(f"City: {item['_id']}, Count: {item['count']}")
else:
    print("\nNo users found that are less than 20 years old.")

