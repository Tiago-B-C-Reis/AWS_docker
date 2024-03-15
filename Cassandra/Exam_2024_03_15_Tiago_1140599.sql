--EXAM with Apache Cassandra.

-- EX:1 CREATE a KEYSPACE/DataBase  ************************************************************************
CREATE KEYSPACE IF NOT EXISTS ecommerce
    WITH REPLICATION = {
        'class': 'SimpleStrategy', 'replication_factor': 1};


-- See all the existing KEYSPACEs
SELECT * FROM system_schema.keyspaces;


-- Use a specific KEYSPACE/DataBase
USE ecommerce;


-- EX:2 CREATE a table: ************************************************************************
CREATE TABLE ecommerce.products (
    product_id INT,
    name TEXT,
    description TEXT,
    price FLOAT,
    stock_quantity INT,
    PRIMARY KEY (product_id)
);


-- EX:3 INSERT data into TABLE ************************************************************************
INSERT INTO ecommerce.products (product_id, name, description, price, stock_quantity) 
VALUES 
    (1, 'Sony', 'Headphones', 100.0, 10);

INSERT INTO ecommerce.products (product_id, name, description, price, stock_quantity) 
VALUES 
    (2, 'Marshall', 'Headphones', 99.90, 20);

INSERT INTO ecommerce.products (product_id, name, description, price, stock_quantity) 
VALUES 
    (3, 'JBL Tune', 'Headphones', 64.90, 20);

INSERT INTO ecommerce.products (product_id, name, description, price, stock_quantity) 
VALUES 
    (4, 'Bose', 'Headphones', 150.0, 10);



-- EX:4 Write a query to get all products. How many records were returned? ************************************************************************
SELECT * FROM ecommerce.products;
-- ANS: all 4 rows, (5 columns each) inserted above, where return.


-- EX:5 Update the stock_quantity for product_id=2. ************************************************************************
UPDATE ecommerce.products 
SET stock_quantity = 35 
WHERE product_id = 2;


-- EX:6 Delete product with id 4; ************************************************************************
DELETE FROM ecommerce.products 
WHERE product_id = 4;


-- EX:7 Select the product with product_id=3. ************************************************************************
SELECT * FROM ecommerce.products 
WHERE product_id = 3;



-- EX:8 products table is good to filter by product_id, but if we want to filter by name? ************************************************************************
--Using the same columns that products table has, create a new table
--(products_by_name), that would efficiently answer that query.
CREATE TABLE ecommerce.products_by_name (
    product_id INT,
    name TEXT,
    description TEXT,
    price FLOAT,
    stock_quantity INT,
    PRIMARY KEY (product_id, name) -- Using "product_id" and "name" as PK to allow efficient query filtering by using "name"
);

INSERT INTO ecommerce.products_by_name (product_id, name, description, price, stock_quantity) 
VALUES 
    (1, 'Sony', 'Headphones', 100.0, 10);
INSERT INTO ecommerce.products_by_name (product_id, name, description, price, stock_quantity) 
VALUES 
    (2, 'Marshall', 'Headphones', 99.90, 20);
INSERT INTO ecommerce.products_by_name (product_id, name, description, price, stock_quantity) 
VALUES 
    (3, 'JBL Tune', 'Headphones', 64.90, 20);
INSERT INTO ecommerce.products_by_name (product_id, name, description, price, stock_quantity) 
VALUES 
    (4, 'Bose', 'Headphones', 150.0, 10);


-- ANS: Adding a name as the primary key will allow efficient query filtering, but with this solution both 
-- "product_id" and "name" need to be used in the query:
SELECT * FROM ecommerce.products_by_name 
WHERE product_id = 1 AND name = 'Sony';

-- This solution below is another way of doing efficient query filtering, but for this case, it is more appropriated 
-- because it allows filtering by only using the PK "name".
CREATE INDEX ON ecommerce.products_by_name (name);

SELECT * FROM ecommerce.products_by_name 
WHERE name = 'Sony';



-- SECTION 2:

-- EX:9 CREATE a table: ************************************************************************
CREATE TABLE ecommerce.orders (
    order_id INT,
    customer_name TEXT,
    product_id INT,
    quantity INT,
    PRIMARY KEY (order_id)
);


-- EX:10 INSERT data into TABLE ************************************************************************
INSERT INTO ecommerce.orders (order_id, customer_name, product_id, quantity) VALUES (1, 'Alice', 1, 2);
INSERT INTO ecommerce.orders (order_id, customer_name, product_id, quantity) VALUES (2, 'John', 1, 1);
INSERT INTO ecommerce.orders (order_id, customer_name, product_id, quantity) VALUES (3, 'Mark', 2, 1);
INSERT INTO ecommerce.orders (order_id, customer_name, product_id, quantity) VALUES (4, 'Thomas', 3, 1);


SELECT * FROM ecommerce.orders ;



-- EX:11 ************************************************************************
-- B)
CREATE TABLE ecommerce.product_orders (
    order_id INT,
    customer_name TEXT,
    product_name TEXT,
    quantity INT,
    PRIMARY KEY (order_id)
);

INSERT INTO ecommerce.product_orders (order_id, customer_name, product_name, quantity) VALUES (1, 'Alice', 'Sony', 2);
INSERT INTO ecommerce.product_orders (order_id, customer_name, product_name, quantity) VALUES (2, 'John', 'Sony', 1);
INSERT INTO ecommerce.product_orders (order_id, customer_name, product_name, quantity) VALUES (3, 'Mark', 'Marshall', 1);
INSERT INTO ecommerce.product_orders (order_id, customer_name, product_name, quantity) VALUES (4, 'Thomas', 'JBL Tune', 1);

-- A)
SELECT 
    order_id, 
    customer_name, 
    product_name, 
    quantity 
FROM ecommerce.product_orders;


-- EX:12 ************************************************************************
CREATE INDEX ON ecommerce.product_orders (customer_name);

SELECT * FROM ecommerce.product_orders 
WHERE customer_name = 'Alice';