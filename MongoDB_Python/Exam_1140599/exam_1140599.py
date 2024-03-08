import pymongo
from pymongo import MongoClient

# 1. Connect to the MongoDB instance
#  connection details
hostname = 'localhost'
port = 27017  # Default MongoDB port
# Create a MongoClient instance
client = MongoClient(hostname, port)
########################################################################## ##########################################################################

########################################################################## ##########################################################################
## Create collection:
# 1. Create DATABASE by [Attribute Based]: (Database Name = "mongo_exercises") (Instance name that represents the DATABASE = "db")
db_instance = client.mongo_exercises 

# 2. Create a Collection: (COLLECTION Name = "movies") (Instance name that represents the COLLECTION = "u_collection")
u_collection = db_instance["movies"]

# 2.a List all Collections inside "movies" Database: (Yes, there is only one collection inside "mongo_exercises", called ['movies'])
print(db_instance.list_collection_names())

## Drop Collection if necessary
u_collection.drop()

# Sample Dataset
documents = (
[
  {
    "title": "Fight Club", "writer": "Chuck Palahniuk", "year": 1999, "actors": ["Brad Pitt", "Edward Norton"]
  },
  {
    "title": "Pulp Fiction", "writer": "Quentin Tarantino", "year": 1994, "actors": ["John Travolta", "Uma Thurman"]
  },
  {
    "title": "Inglorious Basterds", "writer": "Quentin Tarantino", "year": 2009, "actors": ["Brad Pitt", "Diane Kruger", "Eli Roth"]
  },
  {
    "title": "The Hobbit: An Unexpected Journey", "writer": "J.R.R. Tolkein", "year": 2012, "franchise": "The Hobbit"
  },
  {
    "title": "The Hobbit: The Desolation of Smaug", "writer": "J.R.R. Tolkein", "year": 2013, "franchise": "The Hobbit"
  },
  {
    "title": "The Hobbit: The Battle of the Five Armies", "writer": "J.R.R. Tolkein", "year": 2012, "franchise": "The Hobbit",
    "synopsis": "Bilbo and Company are forced to engage in a war against an array of combatants and keep the Lonely Mountain from falling into the hands of a rising darkness."
  },
  {
    "title": "Pee Wee Herman's Big Adventure"
  },
  {
    "title": "Avatar"
  }
])

# 3. Insert data (Many) into "movies" collection:
u_collection.insert_many(documents)




########################################################################## ##########################################################################
## Query/Find Documents:

# 1:
print("All documents: ")
for result in u_collection.find():
    print(result)
    
# 2:
query_writer = {"writer": "Quentin Tarantino"}
for result in u_collection.find(query_writer):
    print(f"all documents with writer set to Quentin Tarantino: {result}")

# 3:
query_actors = {"actors": "Brad Pitt"}
projection_1 ={"_id":0,"actors":1}
for result in u_collection.find(query_actors, projection_1):
    print(f"all documents where actors include Brad Pitt {result}")
    

# 4
query_hobbit = {"franchise": "The Hobbit"}
projection_2 ={"_id":0,"franchise":1}
for result in u_collection.find(query_hobbit, projection_2):
    print(f"all documents with franchise set to The Hobbit {result}")

# 5
query_90s = {"year": {"$gte": 1990, "$lte": 2000}}
for result in u_collection.find(query_90s):
    print(f"all movies released in the 90s (1990-2000) {result}")

# 6
query_year_range = {"$or": [{"year": {"$lt": 2000}}, {"year": {"$gt": 2010}}]}
for result in u_collection.find(query_year_range):
    print(f"all movies released before the year 2000 or after 2010{result}")



########################################################################## ##########################################################################
# Update Documents:

# 1. Add a synopsis to the movie "The Hobbit: An Unexpected Journey"
query_hobbit_journey = {"title": "The Hobbit: An Unexpected Journey"}
update_hobbit_journey = {"$set": {"synopsis": "A reluctant hobbit, Bilbo Baggins, sets out to the Lonely Mountain with a spirited group of dwarves to reclaim their mountain home - and the gold within it - from the dragon Smaug."}}
u_collection.update_one(query_hobbit_journey, update_hobbit_journey)

    
# 2. Add a synopsis to the movie "The Hobbit: The Desolation of Smaug"
query_hobbit_desolation = {"title": "The Hobbit: The Desolation of Smaug"}
update_hobbit_desolation = {"$set": {"synopsis": "The dwarves, along with Bilbo Baggins and Gandalf the Grey, continue their quest to reclaim Erebor, their homeland, from Smaug. Bilbo Baggins is in possession of a mysterious and magical ring."}}
u_collection.update_one(query_hobbit_desolation, update_hobbit_desolation)

# 3. Add an actor named "Samuel L. Jackson" to the movie "Pulp Fiction"
query_pulp_fiction = {"title": "Pulp Fiction"}
update_pulp_fiction = {"$push": {"actors": "Samuel L. Jackson"}}
u_collection.update_one(query_pulp_fiction, update_pulp_fiction)

# check all the update changes:
for result in u_collection.find():
    print(result)



########################################################################## ##########################################################################
# Text Search

# 1. Create a text index on the synopsis field
index = [("synopsis", pymongo.TEXT)]
u_collection.create_index(index)

# 2. Find all movies that have a synopsis that contains the word "Bilbo"
query_bilbo = {"$text": {"$search": "Bilbo"}}
projection_synopsis = {"_id": 0, "synopsis": 1}
print("All movies that have a synopsis that contains the word Bilbo:")
for result in u_collection.find(query_bilbo, projection_synopsis):
    print(result)


# 3. Find all movies that have a synopsis that contains the word "Gandalf"
query_gandalf = {"$text": {"$search": "Gandalf"}}
projection_synopsis1 = {"_id": 0, "synopsis": 1}
print("All movies that have a synopsis that contains the word Gandalf:")
for result in u_collection.find(query_gandalf, projection_synopsis1):
    print(result)



########################################################################## ##########################################################################
# Aggregation:

# 1: Get the number of movies;
print("\nNumber of movies: ", u_collection.count_documents({}))


# 2. Get the number of movies released in the 90s;
pipeline1 = [
    # stage 1
    {
        "$match": {
            "year": {"$gte": 1990, "$lte": 1999} # filter the all 90's years
        }
    },
    # stage 2
    {# count
        "$group": {
            "_id": "$title",  # Group by title
            "count": {"$sum": 1}  # Count documents in each group
        }
    }
]

# Use the [Aggregate] function with the pipeline to instantiate the query result in "aggregation_1"
aggregation_1 = list(u_collection.aggregate(pipeline1))

 # Print the results:
if aggregation_1:
    print("All movies released in the 90s: ")
    for item in aggregation_1:
        print(f"Title: {item['_id']}, Count: {item['count']}")
else:
    print("\nNo users found.")
    

    

# 3. Get the number of movies released before the year 2000 or after 2010;
pipeline2 = [
    {
    # stage 1
        "$match": {
            "$or": [
                {"year": {"$lt": 2000}},
                {"year": {"$gt": 2010}}
            ]
        }
    },
    # stage 2
    {# count
        "$group": {
            "_id": "$title",  # Group by title
            "count": {"$sum": 1}  # Count documents in each group
        }
    }
]

# Use the aggregate function with the pipeline to instantiate the query result in "aggregation_result"
aggregation_2 = list(u_collection.aggregate(pipeline2))

# Print the results
if aggregation_2:
    print("\nNumber of movies released before the year 2000 and after 2010: ")
    for item in aggregation_2:
        print(f"Title: {item['_id']}, Count: {item['count']}")
else:
    print("\nNo movies found released before the year 2000 and after 2010.")
