SELECT *
FROM orders_table;

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::varchar,
ALTER COLUMN store_code TYPE VARCHAR(12) USING store_code::varchar,
ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::varchar,
ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::smallint;

SELECT *
FROM dim_users

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255) USING first_name::varchar,
ALTER COLUMN last_name TYPE VARCHAR(255) USING last_name::varchar,
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
ALTER COLUMN country_code TYPE VARCHAR(3) USING country_code::varchar,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN join_date TYPE DATE USING join_date::date;

SELECT *
FROM dim_store_details;


ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE NUMERIC USING longitude::numeric,
ALTER COLUMN locality TYPE VARCHAR(255) USING locality::varchar,
ALTER COLUMN store_code TYPE VARCHAR(12) USING store_code::varchar,
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
ALTER COLUMN store_type TYPE VARCHAR(255) USING store_type::varchar,
ALTER COLUMN latitude TYPE NUMERIC USING latitude::numeric,
ALTER COLUMN country_code TYPE VARCHAR(3) USING country_code::varchar,
ALTER COLUMN continent TYPE VARCHAR(255) USING continent::varchar;

SELECT *
FROM dim_products;

UPDATE 
  dim_products 
SET 
  product_price = REPLACE(product_price, '£', ''); 

SELECT *
FROM dim_products;

ALTER TABLE dim_products ADD COLUMN weight_class varchar(15);

SELECT *
FROM dim_products;

/*
SELECT *, 
	CASE 
	   WHEN weight < 2 THEN 'Light'
	   WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
	   WHEN weight >= 40 AND weight < 140 THEN 'Mid_Sized'
	   WHEN weight >= 140 THEN 'Truck_Required'
    END AS weight_class
  FROM dim_products;

SELECT *
FROM dim_products;

*/

UPDATE dim_products
SET weight_class = CASE 
   WHEN weight < 2 THEN 'Light'
   WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
   WHEN weight >= 40 AND weight < 140 THEN 'Mid_Sized'
   WHEN weight >= 140 THEN 'Truck_Required'
END;

/*
SELECT *
FROM dim_products
WHERE weight_class = 'Truck_Required';
*/

ALTER TABLE dim_products 
RENAME COLUMN removed TO still_available;

/*
SELECT *
FROM dim_products
WHERE still_available LIKE 'Removed';
*/

UPDATE dim_products
SET still_available = CASE 
   WHEN still_available LIKE 'Removed' THEN FALSE
   ELSE TRUE
END;

SELECT *
FROM dim_products;

/*
SELECT *
FROM dim_products
WHERE still_available != true;
*/

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT(4) USING product_price::float,
ALTER COLUMN weight TYPE FLOAT(5) USING weight::float,
ALTER COLUMN "EAN" TYPE VARCHAR(17) USING "EAN"::varchar,
ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::varchar,
ALTER COLUMN date_added TYPE DATE USING date_added::date,
ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
ALTER COLUMN still_available TYPE BOOL USING still_available::bool,
ALTER COLUMN weight_class TYPE VARCHAR(15) USING weight_class::varchar;

SELECT *
FROM dim_products;


SELECT *
FROM dim_date_times;

/*
SELECT *
FROM dim_date_times
WHERE date_uuid = '3b7ca996-37f9-433f-b6d0-ce8391b615ad';

SELECT *
FROM dim_date_times
WHERE date_uuid = '3A21WYQSY7';

SELECT *
FROM dim_date_times
WHERE date_uuid = 'NULL';
*/

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2) USING month::varchar,
ALTER COLUMN year TYPE VARCHAR(4) USING year::varchar,
ALTER COLUMN day TYPE VARCHAR(2) USING day::varchar,
ALTER COLUMN time_period TYPE VARCHAR(10) USING time_period::varchar,
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

SELECT *
FROM dim_date_times;


SELECT *
FROM dim_card_details;

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::varchar,
ALTER COLUMN expiry_date TYPE VARCHAR(5) USING expiry_date::varchar,
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;

SELECT *
FROM dim_card_details;

/* Task 8 */
SELECT *
FROM orders_table;

ALTER TABLE dim_users 
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_store_details 
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_products 
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_date_times 
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details 
ADD PRIMARY KEY (card_number);

/* Task 9 */
SELECT *
FROM orders_table;

SELECT *
FROM dim_card_details;

ALTER TABLE orders_table
	ADD CONSTRAINT fk_users
	FOREIGN KEY (user_uuid) 
	REFERENCES dim_users (user_uuid);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_store
		FOREIGN KEY (store_code) 
		REFERENCES dim_store_details (store_code);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_product_code
		FOREIGN KEY (product_code) 
		REFERENCES dim_products (product_code);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_date_uuid
		FOREIGN KEY (date_uuid) 
		REFERENCES dim_date_times (date_uuid);

ALTER TABLE orders_table
	ADD CONSTRAINT fk_card_number
		FOREIGN KEY (card_number) 
		REFERENCES dim_card_details (card_number);

SELECT * FROM orders_table WHERE card_number = '6011036876440620' ;
SELECT * FROM dim_card_details WHERE card_number = '6011036876440620' ;

/* test */

SELECT * FROM orders_table
WHERE card_number NOT IN(SELECT dim_card_details.card_number FROM dim_card_details);