# Calculate the total price for all orders;

pipeline = [
    # stage 1
    {
        # add a new column with the total price
        '$addFields': {
            'total_price': {'$multiply': ['$price', '$quantity']}
        }
    },
    # stage 2
    {
        # summ the total price
        '$group': {
            '_id': None, # Grouping without a specific field to calculate total for all documents
            'total_price_for_all': {'$sum': '$total_price'}
        }
    }
]

# Execute the aggregation pipeline
results = db.orders.aggregate (pipeline)
# Print the results
for result in results:
    print(result)