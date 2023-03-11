DROP TABLE IF EXISTS users10;
CREATE TABLE users10 (
    id serial primary key,
    name text,
    email VARCHAR,
    age integer
);

DROP TABLE IF EXISTS my_orders;
CREATE TABLE my_orders (
    id BIGSERIAL primary key,
    user_id integer,
    order_date date,
    order_amount float,
    order_updated timestamp without time zone
);

DROP TYPE IF EXISTS park_type CASCADE;
CREATE TYPE PARK_TYPE AS ENUM ('urban', 'country', 'dog', 'kids');

DROP TABLE IF EXISTS local_parks;
CREATE TABLE local_parks (
    park_name text,
    park_type PARK_TYPE,
    park_size integer,
    park_location_lat float,
    park_location_lon float
);