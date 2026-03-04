#Data Cleaning Project using real life AirBnB data for the city of Cape Town
#At a glance the table listings contains details of AirBnB rental sites in Cape Town however the data is not usable yet due to null fields and other inconsistencies

#First, I create backup tables of the raw data which will be used to perform data cleaning and analysis.

create table b_listings like listings;
insert b_listings select * from listings;

# the following code will remove any trailing spaces in order to standardize data in columns
SET SQL_SAFE_UPDATES = 0;

Update b_listings
SET b_listings.id = trim(b_listings.id),
b_listings.name = trim(b_listings.name),
b_listings.host_id = trim(b_listings.host_id),
b_listings.host_name = trim(b_listings.host_name),
b_listings.neighbourhood_group = trim(b_listings.neighbourhood_group),
b_listings.neighbourhood = trim(b_listings.neighbourhood),
b_listings.latitude = trim(b_listings.latitude),
b_listings.longitude = trim(b_listings.longitude),
b_listings.room_type = trim(b_listings.room_type),
b_listings.price = trim(b_listings.price),
b_listings.minimum_nights = trim(b_listings.minimum_nights),
b_listings.number_of_reviews = trim(b_listings.number_of_reviews),
b_listings.last_review = trim(b_listings.last_review),
b_listings.reviews_per_month = trim(b_listings.reviews_per_month),
b_listings.calculated_host_listings_count = trim(b_listings.calculated_host_listings_count),
b_listings.availability_365 = trim(b_listings.availability_365),
b_listings.number_of_reviews_ltm = trim(b_listings.number_of_reviews_ltm),
b_listings.license = trim(b_listings.license);

SET SQL_SAFE_UPDATES = 1;


#Next we find and remove duplicates

SELECT b_listings.id, COUNT(*) AS occurrences
FROM b_listings
GROUP BY b_listings.id
HAVING COUNT(*) > 1;

#the above will determine if there are any duplicates with the listings ID, fortunately there are none



#Next, we determine if there are any null values 
Select * from b_listings WHERE b_listings.id IS NULL OR
b_listings.name IS NULL OR
b_listings.host_id IS NULL OR
b_listings.host_name IS NULL OR
b_listings.neighbourhood_group IS NULL or
b_listings.neighbourhood IS NULL OR
b_listings.latitude IS NULL OR
b_listings.longitude IS NULL OR
b_listings.room_type IS NULL OR
b_listings.price IS NULL OR
b_listings.minimum_nights IS NULL OR
b_listings.number_of_reviews IS NULL OR
b_listings.last_review IS NULL OR
b_listings.reviews_per_month IS NULL OR
b_listings.calculated_host_listings_count OR
b_listings.availability_365 IS NULL OR 
b_listings.number_of_reviews_ltm IS NULL OR
b_listings.license IS NULL;

#the entirety of neighbourhood_group is determined to be null, and the licence column has only two entries

Select * from b_listings WHERE b_listings.id IS NULL OR
b_listings.name IS NULL OR
b_listings.host_id IS NULL OR
b_listings.host_name IS NULL OR
b_listings.neighbourhood IS NULL OR
b_listings.latitude IS NULL OR
b_listings.longitude IS NULL OR
b_listings.room_type IS NULL OR
b_listings.price IS NULL OR
b_listings.minimum_nights IS NULL OR
b_listings.number_of_reviews IS NULL OR
b_listings.last_review IS NULL OR
b_listings.reviews_per_month IS NULL OR
b_listings.calculated_host_listings_count OR
b_listings.availability_365 IS NULL OR 
b_listings.number_of_reviews_ltm IS NULL;

#checking for other columns reveals tere is missing data in the price column
#to ensure we can still work with these listings we will insert a temporary price into these listings using the avergae price of the neighbourhood

select * from b_listings where b_listings.price IS NULL OR b_listings.price = '';
select * from b_listings order by b_listings.neighbourhood;

select b_listings.neighbourhood, 
round(avg(b_listings.price) over (partition by b_listings.neighbourhood),0) as
"Avg price" from b_listings; 

select b_listings.neighbourhood, 
round(avg(b_listings.price),0) as 
"avg_price" from b_listings group by b_listings.neighbourhood;

select * from b_listings;
#either of the above can be used to determine the average prices for each neighbourhood, next we update the null fields in price with the averages

#now we use a join statement to update the NULL values in price

SET SQL_SAFE_UPDATES = 0;

UPDATE b_listings 
JOIN (
SELECT b_listings.neighbourhood, ROUND(AVG(b_listings.price),0) AS avg_price FROM b_listings
WHERE b_listings.price IS NOT NULL
GROUP BY b_listings.neighbourhood
) tbl_avg #this creates a derived table for the results of the query, this hold all values and columns from the join subquery
ON b_listings.neighbourhood = tbl_avg.neighbourhood
SET b_listings.price = tbl_avg.avg_price
WHERE b_listings.price IS NULL OR b_listings.price = '';

SET SQL_SAFE_UPDATES = 1;

#The neighbourhood groups column is empty and serves no purpose, so I remove it

ALTER TABLE b_listings DROP COLUMN neighbourhood_group;

#next I validate the data types for each column 

describe b_listings;

#from this sql, we can see that price is currently text, last review and reviews per month are also text
#price should be converted to decimal, last review should be a date format, and reviews per month should be int
	
Alter table b_listings modify column price DECIMAL(10,2);
SELECT last_review from b_listings;

ALTER TABLE b_listings modify column last_review DATE;
#this command fails due to the null values, therefore we create a new last_review column allowing for null values

ALTER TABLE b_listings ADD COLUMN last_review_b DATE NULL;

SET SQL_SAFE_UPDATES = 0;
update b_listings set last_review_b = str_to_date(nullif(last_review, ''), '%Y-%m-%d') where last_review IS NOT NULL;
SET SQL_SAFE_UPDATES = 1;

# NOW that we have a valid date column, we can drop the previous column

alter table b_listings drop column last_review;

#the final column to convert is the reviews_per_month column

SET SQL_SAFE_UPDATES = 0;
Update b_listings set reviews_per_month = '0' where reviews_per_month IS NULL OR reviews_per_month = ''; 
SET SQL_SAFE_UPDATES = 0;

alter table b_listings modify column reviews_per_month INT NULL;

#the final column to fix is the licence column

SELECT COUNT(*)
FROM b_listings
WHERE license IS NULL OR license = '';

#from this we see that nearly all of the columns are null and will not be useful for analysis

#The next major step would be to normalize the database
#From repeated values, we can create: 1. A host table, a review table

CREATE TABLE b_hosts AS SELECT distinct host_id, host_name, calculated_host_listings_count from b_listings;
alter table b_listings drop column host_name, drop column calculated_host_listings_count;


ALTER TABLE b_listings ADD PRIMARY KEY (id);

CREATE TABLE b_reviews(
listing_id int primary key, number_of_reviews int, reviews_per_month int,
 number_of_reviews_ltm int, last_review_b date, 
foreign key(listing_id) references  b_listings(id));

Insert into b_reviews 
select ID, number_of_reviews, reviews_per_month, number_of_reviews_ltm, last_review_b from b_listings;

alter table b_listings drop column number_of_reviews, drop column reviews_per_month, drop column number_of_reviews_ltm, drop column last_review_b;



    