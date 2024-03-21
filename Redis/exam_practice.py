from redis import Redis

# All responses are returned as bytes in Python.
# To receive decoded strings, set decode_responses=True
# create redis objects
redis = Redis (host='localhost', port='6379', decode_responses=True)


##################################### PART I == Strings #########################################################
# create a new key - value 
redis.set('foo', 'bar')
# get the value for the key created
value = redis.get('foo')
print(f"Key: foo | Value: {value}")

# create a new key - value
redis.mset({'fool': 'bar1', 'foo2': 'bar2'}) 
# get the value for the key created 
value = redis.mget('fool', 'foo2') 
print (f"Key: fool | Value: {value[0]}")
print (f"Key: foo2 | Value: {value[1]}")



##################################### PART II == Sets #########################################################
res1 = redis.sadd("bikes:racing:france", "bike:1") ## Create Set Key and add Value
print(res1) # >>> 1
res2 = redis.sadd("bikes:racing:france", "bike:1")
print(res2) # >>> 0
res3 = redis.sadd("bikes:racing:france", "bike: 2", "bike:3")
print(res3) # >>> 2
res4 = redis.sadd("bikes:racing:usa", "bike: 1", "bike:4") ## Create Set Key and add Value
print(res4) # >>> 2

# check if bike:1 is a member of bikes: racing:usa
res5 = redis.sismember("bikes:racing:usa", "bike:1") 
print(res5) # >>> 1
#checkif bike: 2 is a member of bikes: racing:usa
res6 = redis.sismember ("bikes:racing:usa", "bike:2") 
print(res6) # >>> 0
# intersection get the values that exists in both sets
res7= redis.sinter("bikes:racing:france", "bikes: racing:usa") 
print(res7)
# get the number of elements in set res = redis.scard("bikes: racing: france") print(res) # >>> 3
res8= redis.scard("bikes:racing:france") 
print(res8)
# Move member from the set at source to the set at destination. 
res9 = redis.smove("bikes:racing:france", "bikes:racing:usa", "bike:3")
print(res9)



##################################### PART III == Sorted Sets ###################################################
# add value Norem with score 10
res1 = redis.zadd("racer_scores", {"Norem": 10}) 
print(res1) # >>> 1

# add value Castilla with score 12
res2 = redis.zadd("racer_scores", {"Castilla": 12}) 
print(res2) # >>> 1

res3 = redis.zadd("racer_scores", {"Sam-Bodden": 8, "Royce": 10, "Ford": 6, "Prickett": 14, "Castilla": 12},)
print(res3) # >>> 4


# ZRANGE order is low to high
res4 = redis.zrange("racer_scores", 0, -1) 
print(res4) # >>> [ 'Ford', 'Sam-Bodden', 'Norem', 'Royce', 'Castilla', 'Prickett']

# ZREVRANGE order is high to low:
res5 = redis.zrevrange("racer_scores", 0, -1)
print(res5) # >>> ['Prickett', 'Castilla', 'Royce', 'Norem', 'Sam-Bodden', 'Ford']

# all the racers with 10 or fewer points
res6 = redis.zrangebyscore ("racer_scores", "-inf", 10) 
print(res6) # >>> ['Ford', 'Sam-Bodden', 'Norem', 'Royce']]

# Returns the sorted set cardinality (number of elements) of the sorted set stored at key.
res7 = redis.zcard("racer_scores")
print(res7)

# Returns the number of elements in the sorted set at key with a score between min and max.
res8 = redis.zcount("racer_scores", 0, 10)
print(res8)


##################################### PART IV == Hash #########################################################
key = "bike: 1"
data = {
    "model": "Deimos",
    "brand": "Ergonom",
    "type": "Enduro bikes", 
    "price": 4972,
}
res1 = redis.hset(
    key,
    mapping=data,
    )
print(f"Key: {key} | Number of values inserted: {res1}")

# Add year = 2000 to the key “bike:1”
redis.hset(key, "year", 2000)

# Return the value for the model in the key “bike:1”
res2 = redis.hget("bike: 1", "model")
print (f"Key: {key} | Field: model | Value: {res2}")

# ●Return the value for the price in the key “bike:1”
res3 = redis.hget("bike: 1", "price")
print (f"Key: {key} | Field: price | Value: {res3}")

# Return all the fields in the value for the key “bike:1”
res4 = redis.hgetall("bike: 1")
print(res4)

# to return more than one field # returns an array of values
# Return the value for the fields model and price, in the same command, for the key “bike:1”
res5 = redis.hmget("bike: 1", ["model", "price"]) 
print(res5)
# >>> ['Deimos', '4972']

# Returns all field names in the hash stored at key. (https://redis.io/commands/?group=hash)
res6 = redis.hkeys("bike: 1")
print(res6)


##################################### PART IV == Lists #########################################################
key = 'bikes'
redis.rpush(key, 'bike:1')

# Add more values to the key “bike:2”, “bike:3”
data_array = ['bike:2', 'bike:3']
redis.rpush(key, *data_array)

# Show all the elements in the list;
print(f"Key: {key} | Elements: {redis.lrange(key,0,-1)}")

# Get the length of the list;
print(f"Key: {key} | Length: {redis. llen (key)}")

# removes one element in the right
value = redis.rpop (key)
print(f"Key: {key} | Element removed: {value}")
print(f"Key: {key} | Current Elements: {redis.lrange (key, 0, -1)}") 
print(f"Key: {key} | Length: {redis.llen (key)}")

# Add more two elements to the end of the list “bike:4”, “bike:5”
res = redis.rpush(key, "bike:4", "bike:5")
print(f"Key: {key} |Current Elements: {redis.lrange(key, 0, -1)}")

# Remove one elements from the beginning of the list
redis.ltrim(key, 1, -1)
print(f"Key: {key} | Current Elements: {redis.lrange (key, 0, -1)}") 
print(f"Key: {key} | Length: {redis.llen (key)}")