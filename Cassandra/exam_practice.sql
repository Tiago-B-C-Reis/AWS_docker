--EXAM practice with Apache Cassandra.

-- CREATE a KEYSPACE/DataBase
CREATE KEYSPACE IF NOT EXISTS bookstore
    WITH REPLICATION = {
        'class': 'SimpleStrategy', 'replication_factor': 1};


-- See all the existing KEYSPACEs
SELECT * FROM system_schema.keyspaces;


-- Use a specific KEYSPACE/DataBase
USE bookstore;


-- CREATE a table:
create table books (
	book_id INT,
	title TEXT,
	author TEXT,
	genre TEXT,
	price FLOAT
PRIMARY KEY (book_id)
);



-- INSERT data into TABLE
INSERT INTO bookstore.books (book_id, title, author, genre, price) 
VALUES 
    (1, 'The Great Adventure', 'Alex Reed', 'Adventure', 15.99);

INSERT INTO bookstore.books (book_id, title, author, genre, price) 
VALUES 
    (2, 'Mystery of the Ancients', 'Maria Garcia', 'Mystery', 12.50);

INSERT INTO bookstore.books (book_id, title, author, genre, price) 
VALUES 
    (3, 'The Last Stand', 'John Doe', 'Action', 18.75);

INSERT INTO bookstore.books (book_id, title, author, genre, price) 
VALUES 
    (4, 'Journey Through Time', 'Emily White', 'Sci-Fi', 9.99);

INSERT INTO bookstore.books (book_id, title, author, genre, price) 
VALUES 
    (5, 'Secrets of the Deep', 'David Smith', 'Thriller', 20.0);



-- CREATE a table:
CREATE TABLE bookstore.orders (
    order_id INT,
    customer_name TEXT,
    email TEXT,
    book_id INT,
    quantity INT,
    order_date DATE,
    PRIMARY KEY (order_id)
);


-- INSERT data into TABLE
INSERT INTO bookstore.orders (order_id, customer_name, email, book_id, quantity, order_date) 
VALUES 
    (1, 'Alex', 'alex@sample.com', 3, 2, '2024-01-10');

INSERT INTO bookstore.orders (order_id, customer_name, email, book_id, quantity, order_date) 
VALUES 
    (2, 'Tony', 'tony@sample.com', 4, 1, '2024-01-10');

INSERT INTO bookstore.orders (order_id, customer_name, email, book_id, quantity, order_date) 
VALUES 
    (3, 'Jennifer', 'jennifer@sample.com', 1, 1, '2024-02-01');

INSERT INTO bookstore.orders (order_id, customer_name, email, book_id, quantity, order_date) 
VALUES 
    (4, 'Eric', 'eric@sample.com', 3, 1, '2024-03-04');

INSERT INTO bookstore.orders (order_id, customer_name, email, book_id, quantity, order_date) 
VALUES 
    (5, 'Eric', 'eric@sample.com', 2, 1, '2024-02-15');



-- QUERIES: --------------------------------------------------------------------------------------------

-- Return all books:
SELECT * FROM bookstore.books;

--Update the price for book_id 5 to 24.99:
-- If a PK is in the "SET" statement, then it is NOT going to work because Cassandra doesn't allow to update on PK.
UPDATE bookstore.books 
SET price = 24.99 
WHERE book_id = 5;


-- Return all books with the genre `Adventure`: ----------- *** INDEX *** --------------
-- INDEX METODH 1:
CREATE INDEX ON bookstore.books(genre);

SELECT * FROM bookstore.books
WHERE genre = 'Adventure';

-- INDEX METODH 2: (NOT the most efficient)
	-- When CREATing the table, reference the column to use as filter in the PK:
		--  (...) PRIMARY KEY (order_id, genre) (...)

-- INDEX METODH 3: (NOT the most efficient)
SELECT * FROM bookstore.books
WHERE genre = 'Adventure' ALLOW FILTERING;


-- Return all orders done on `2024-01-10`:
CREATE INDEX ON bookstore.orders(order_date);

SELECT * FROM bookstore.orders
WHERE order_date = '2024-01-10';



-- Perform a JOIN in Cassandra: ---------------------------------------------------------------
CREATE TABLE bookstore.orders_with_books (
    order_id INT,
    customer_name TEXT,
    email TEXT,
    book_id INT,
    title TEXT,
    PRIMARY KEY (order_id, book_id)
);

INSERT INTO bookstore.orders_with_books (order_id, customer_name, email, book_id, title) 
VALUES 
    (1, 'Alex', 'alex@sample.com', 3, 'The Last Stand') IF NOT EXISTS;

INSERT INTO bookstore.orders_with_books (order_id, customer_name, email, book_id, title) 
VALUES 
    (2, 'Tony', 'tony@sample.com', 4, 'Secrets of the Deep') IF NOT EXISTS;

INSERT INTO bookstore.orders_with_books (order_id, customer_name, email, book_id, title) 
VALUES 
    (3, 'Jennifer', 'jennifer@sample.com', 'The Great Adventure') IF NOT EXISTS;

INSERT INTO bookstore.orders_with_books (order_id, customer_name, email, book_id, title) 
VALUES
    (4, 'Eric', 'eric@sample.com', 3, 'The Last Stand') IF NOT EXISTS;

INSERT INTO bookstore.orders_with_books (order_id, customer_name, email, book_id, title) 
VALUES
    (5, 'Eric', 'eric@sample.com', 2, 'Mystery of the Ancients') IF NOT EXISTS;

SELECT 
    order_id, 
    customer_name, 
    email, 
    title 
FROM bookstore.orders_with_books;





 ------------ **** Expiring data with time-to-live (TTL) *** -----------------------------

 