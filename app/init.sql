CREATE TABLE IF NOT EXISTS object_levels (
    level integer PRIMARY KEY, 
    name varchar(200) NOT NULL
);


CREATE TABLE IF NOT EXISTS objects (
    id serial PRIMARY KEY, 
    level integer NOT NULL, 
    typename varchar(200) NOT NULL, 
    name varchar(200) NOT NULL
);


CREATE TABLE IF NOT EXISTS date_version (
    version varchar(200) NOT NULL
);