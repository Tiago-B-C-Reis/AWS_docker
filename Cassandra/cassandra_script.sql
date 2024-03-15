-------------------- Aula 1 ---------------------- 

-- describe keyspaces

SELECT * FROM system_schema.keyspaces;

CREATE KEYSPACE IF NOT EXISTS first_keyspace
    WITH REPLICATION = {
        'class': 'SimpleStrategy', 'replication_factor': 1};


Describe keyspaces;


DROP KEYSPACE IF EXISTS first_keyspace;


Alter KEYSPACE first_keyspace
    WITH REPLICATION = {
        'class': 'SimpleStrategy', 'replication_factor': 1};



CREATE TABLE [IF NOT EXISTS] [keyspace_name.] table_name ( 
	column_definition [,...]
	PRIMARY KEY (column_name [, column_name ...])
[WITH table_options
	| CLUSTERING ORDER BY (clustering_column_name order])
	| ID = 'table_hash_tag'
	| COMPACT STORAGE]


ALTER TABLE [keyspace_name.] table_name 
	[ALTER column_name TYPE cql_type]
	[ADD (column_definition_list)]
	[DROP column_list | COMPACT STORAGE ] 
	[RENAME column_name TO column_name] 
	[WITH table_properties];

DROP TABLE [IF EXISTS] keyspace_name.table_name;

describe tables;

Describe keyspaces;




-------------------- Aula 2 ---------------------- 

CREATE KEYSPACE IF NOT EXISTS SocialMedia
    WITH REPLICATION = {
        'class': 'SimpleStrategy', 'replication_factor': 1};

SELECT * FROM system_schema.keyspaces;

use SocialMedia;


create table users (
	user_id int,
	email VARCHAR,
	name VARCHAR,
	creation_date date,
PRIMARY KEY (user_id)
);


insert into socialmedia.users (user_id, email, name, creation_date) VALUES (1, 'alice@example.com', 'Alice Smith', '2024-01-01'); 
insert into socialmedia.users (user_id, email, name, creation_date) VALUES (2, 'bob@example.com', 'Bob Johnson', '2024-01-02'); 
insert into socialmedia.users (user_id, email, name, creation_date) VALUES (3, 'carol@example.com', 'Carol Davis', '2024-01-03'); 
insert into socialmedia.users (user_id, email, name, creation_date) VALUES (4, 'dave@example.com', 'Dave Martinez', '2024-01-04');

SELECT * FROM users;



SELECT
	select_expression | DISTINCT partition
FROM [keyspace_name.] table_name
[WHERE partition_value
	[AND clustering_filters
	[AND static_filters]]]
[ORDER BY PK_column_name ASC DESC]
[LIMIT N]
[ALLOW FILTERING]



SELECT event_id,
	dateOf(created_at) AS creation_date,
	blobAsText(content) AS content
FROM timeline;


SELECT * FROM socialmedia.users WHERE user_id = 1;
SELECT * FROM socialmedia.users WHERE email = 'alice@example.com' ALLOW FILTERING;

CREATE INDEX ON socialmedia.users(email);

SELECT * FROM socialmedia.users WHERE email = 'alice@example.com';


-- ADD the column we want to be able to use as WHERE filter
create table users2 (
	user_id int,
	email VARCHAR,
	name VARCHAR,
	creation_date date,
PRIMARY KEY (user_id, email)
);

insert into socialmedia.users2 (user_id, email, name, creation_date) VALUES (1, 'alice@example.com', 'Alice Smith', '2024-01-01'); 
insert into socialmedia.users2 (user_id, email, name, creation_date) VALUES (2, 'bob@example.com', 'Bob Johnson', '2024-01-02'); 
insert into socialmedia.users2 (user_id, email, name, creation_date) VALUES (3, 'carol@example.com', 'Carol Davis', '2024-01-03'); 
insert into socialmedia.users2 (user_id, email, name, creation_date) VALUES (4, 'dave@example.com', 'Dave Martinez', '2024-01-04');

SELECT * FROM users2;

SELECT * FROM socialmedia.users2 
WHERE user_id = 1 AND email = 'alice@example.com';

SELECT 
	email, 
	name 
FROM socialmedia.users2;


CREATE INDEX ON socialmedia.users2(creation_date);


SELECT 
	email, 
	name 
FROM socialmedia.users2
WHERE creation_date = '2024-01-03';



-- This does not work because the ID = 4 already exists and we have the condition "IF NOT EXISTS"
INSERT INTO socialmedia.users2 (user_id, email, name, creation_date) 
VALUES (4, 'dave@example.com', 'Dave Martinez Smith', '2024-01-04') IF NOT EXISTS;

-- This one below only works if I update the table "users" that has only the primary key "user_id", because on "users2" it has also an e-mail as PK and it does not allow to update a PK.
UPDATE socialmedia.users
SET email = 'dave@example.com', name = 'Dave Martinez Smith', creation_date = '2024-01-04' 
WHERE user_id = 4 IF EXISTS;


DELETE FROM socialmedia.users2 WHERE user_id = 4;

INSERT INTO socialmedia.users2 (user_id, email, name, creation_date)
VALUES (4, 'dave@example.com', 'Dave Martinez Smith', '2024-01-04');






 ------------ **** Expiring data with time-to-live (TTL) *** -----------------------------

CREATE TABLE chat (
	id INT,
	messages TEXT,
	message_by TEXT,
	time TIMESTAMP,
PRIMARY KEY (id)
) 
WITH default_time_to_live = 10; -- with this the data is deleted 10 seconds after inserted



INSERT INTO socialmedia.chat (id, messages, message_by, time) 
VALUES (1, 'Text message - 1', 'John', toTimestamp(now()));


SELECT * FROM chat;


INSERT INTO socialmedia.chat (id, messages, message_by, time) 
VALUES (2, 'Text message - 2', 'John', toTimestamp(now())) USING TTL 30; -- This overwrite the TTL

INSERT INTO socialmedia.chat (id, messages, message_by, time) 
VALUES (3, 'Text message - 3', 'John', toTimestamp(now())); -- This uses the default TTL


INSERT INTO socialmedia.chat (id, messages, message_by, time) 
VALUES (4, 'Test message - 4', 'John', toTimestamp(now()));


UPDATE socialmedia.chat 
USING TTL 30
	SET messages = 'Message - 4'
	WHERE id = 4 IF EXISTS;







