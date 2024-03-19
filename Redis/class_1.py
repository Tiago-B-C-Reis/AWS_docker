from redis import Redis

# All responses are returned as bytes in Python.
# To receive decoded strings, set decode_responses=True
redis = Redis (host='127.0.0.1', port='6379', decode_responses=True)
# redis = Redis (host='localhost', port='6379', decode_responses=True) (does the same as the one above)
print (f"Redis version: {redis.info() ['redis_version']}")



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