create database ecom;
use ecom;
select column_name, data_type 
from INFORMATION_SCHEMA.columns where table_name = 'product_category_name_translation';

select CONVERT(VARCHAR(10), order_purchase_timestamp, 111) from olist_orders_dataset;

select count (*) from olist_orders_dataset;
select count (*) from olist_order_payments_dataset
select count (*) from olist_order_items_dataset
select count (*) from olist_order_reviews_dataset
select count (customer_id), count (customer_unique_id) from olist_customers_dataset;
--column customer_id = customer_unique_id

--1.1 number of customers each month
create table #olist_customers_date 
(order_id nvarchar(100), 
customer_id nvarchar(100), 
order_purchase_timestamp datetime2, 
order_status nvarchar(50), 
order_purchase_date_ date)

insert into #olist_customers_date 
select a.order_id,
a.customer_id, 
a.order_purchase_timestamp,
a.order_status,
a.order_purchase_date_
from
    (select *, CONVERT(VARCHAR(10), order_purchase_timestamp, 111) as order_purchase_date_
    from olist_orders_dataset) a 
    left join 
    (select customer_id
    from olist_customers_dataset) b 
    on a.customer_id = b.customer_id

select top 10 * from #olist_customers_date;

select count (distinct customer_id), count (customer_id) from olist_orders_dataset;
--each customer has only one time of order 
--all of them are considered to be new customers to the company
-- metric of customer = metric of new customer
--the company should extend the timeframe of data collection
--or re-design their way of data collecting and data model
--to see if this customer base
--has any behavior of reorder
--this action will give the company a better look on customer engagement

with olist_customers_month as 
    (select customer_id, min(order_purchase_date_) as first_order_date
    from #olist_customers_date
    group by customer_id)
, olist_customers_new_month as 
    (select
    year(first_order_date) year,
    month(first_order_date) month,
    customer_id
    from olist_customers_month)
, olist_customers_new_month_agg as
    (select year, month, count (customer_id) as num_customers
    from olist_customers_new_month
    group by year, month) 
, olist_customers_new_month_lead as 
(select year, month, num_customers, 
lead(num_customers, 1,0) over(order by year, month) next_num_customers 
from olist_customers_new_month_agg) 
select *,
round(((cast(next_num_customers as float) - cast(num_customers as float))/cast(num_customers as float)),1) as cust_growth_ratio 
from olist_customers_new_month_lead;


--2. total value, value per customer and value_per_order
select count (order_id), count (distinct order_id)
from olist_order_payments_dataset;
select count (order_id)
from olist_orders_dataset;
--not all order_id having payment value are order_id contained in orders_dataset

--2.1 total value and growth_ratio (using lead() to calculate)
with olist_order_payment_detail as
    (select a.order_id, 
    b.customer_id,
    a.payment_value, 
    b.order_purchase_date_
    from
    (select order_id, payment_value 
    from olist_order_payments_dataset) a 
    left join 
    (select *, CONVERT(VARCHAR(10), order_purchase_timestamp, 111) as order_purchase_date_
    from olist_orders_dataset) b 
    on a.order_id = b.order_id)
, olist_order_payment_agg as
    (select 
    year(order_purchase_date_) year,
    month(order_purchase_date_) month, 
    round(sum(payment_value),1) total_value,
    count (distinct customer_id) num_customer,
    round(sum(payment_value)/count (distinct customer_id),1) value_per_cust,
    round(sum(payment_value)/count (distinct order_id),1) value_per_order
    from olist_order_payment_detail 
    group by 
    year(order_purchase_date_),
    month(order_purchase_date_))
, olist_order_payment_lead as
    (select year, month, total_value,
    lead (total_value, 1,0) over (order by year, month) next_total_value 
    from olist_order_payment_agg)
select year, month, total_value, 
round((next_total_value-total_value)/total_value,1) as value_growth_ratio
from olist_order_payment_lead 
order by year, month;

--tang truong rat co van de

select top 10 * from olist_order_payments_dataset;
select count (order_id), count (distinct order_id) 
from olist_order_payments_dataset;

--2.2 value per customer and value per order
with olist_order_payment_detail as
    (select a.order_id, 
    b.customer_id,
    a.payment_value, 
    b.order_purchase_date_
    from
    (select order_id, payment_value 
    from olist_order_payments_dataset) a 
    left join 
    (select *, CONVERT(VARCHAR(10), order_purchase_timestamp, 111) as order_purchase_date_
    from olist_orders_dataset) b 
    on a.order_id = b.order_id)
, olist_order_payment_agg as
    (select 
    year(order_purchase_date_) year,
    month(order_purchase_date_) month, 
    round(sum(payment_value)/count (customer_id),1) value_per_cust,
    round(sum(payment_value)/count (order_id),1) value_per_order
    from olist_order_payment_detail 
    group by 
    year(order_purchase_date_),
    month(order_purchase_date_))
select * from olist_order_payment_agg
order by year, month;

--3.1 service quality: review score
--the comments are written in Portuguese, the analyst cannot translate them at the moment of the test
select count (distinct order_id), count (order_id)
from olist_order_reviews_dataset;

create table #olist_service_quality
(order_id nvarchar(100), 
review_id nvarchar(100), 
review_score tinyint, 
review_comment_title nvarchar(100), 
review_comment_message nvarchar(max), 
order_status nvarchar(50),
order_purchase_timestamp date, 
order_delivered_customer_date date,
order_estimated_delivery_date date) 

insert into #olist_service_quality
select a.*, 
order_status,
order_purchase_timestamp, 
order_delivered_customer_date,
order_estimated_delivery_date 
from 
(select order_id, review_id, review_score, review_comment_title,
review_comment_message
from olist_order_reviews_dataset) a 
left join 
(select order_id, 
order_status, 
CONVERT(VARCHAR(10), order_purchase_timestamp, 111) order_purchase_timestamp, 
CONVERT(VARCHAR(10), order_delivered_customer_date, 111) order_delivered_customer_date,
CONVERT(VARCHAR(10), order_estimated_delivery_date, 111) order_estimated_delivery_date
from olist_orders_dataset) b 
on a.order_id = b.order_id;

select top 10 * from #olist_service_quality


--3.2 chi so chenh lech giua ngay purchase va ngay deliver thuc te
--chenh lech giua ngay deliver thuc te va ngay estimate 
--=> tu day phai bien thanh ty le ntn de nhin duoc pattern?

--=> lam them phan tich giai thich ve review_score:
--thgian van chuyen 
--tinh chat hang hoa: gia thanh, kich thuoc
--chung loai hang hoa

create table #olist_service_quality_review
(order_id nvarchar(100), 
review_id nvarchar(100), 
review_score tinyint, 
review_comment_title nvarchar(100), 
review_comment_message nvarchar(max), 
order_status nvarchar(50),
order_purchase_timestamp date, 
order_delivered_customer_date date,
order_estimated_delivery_date date,
price float, 
freight_value float, 
product_category_translated_name nvarchar(50),
product_photos_qty int, 
product_weight_g int, 
product_size_cm3 int)


insert into #olist_service_quality_review
select a.*, 
b.price, 
freight_value,
product_category_translated_name,
product_photos_qty,
product_weight_g,
product_size_cm3
from 
#olist_service_quality a 
left join 
(select order_id, product_id, 
price, 
freight_value
from olist_order_items_dataset) b 
on a.order_id = b.order_id 
left join 
(select product_id, product_category_name,
product_photos_qty,
product_weight_g,
(cast(product_length_cm as int) * cast(product_height_cm as int)*cast(product_width_cm as int)) as product_size_cm3
from olist_products_dataset) c
on b.product_id = c.product_id 
left join
product_category_name_translation d 
on c.product_category_name = d.product_category_name


--avg score
select avg(review_score) avg_score, 
count (review_id) num_review, count (order_id)
num_order_w_review, count (distinct order_id) 
num_order
from #olist_service_quality
where review_score is not null;

--difference between dates of delivery
select order_id, 
review_score,
datediff(day, order_purchase_timestamp, order_delivered_customer_date) diff_purchase_deliver,
datediff(day, order_delivered_customer_date, order_estimated_delivery_date) diff_deliver_estimate
from #olist_service_quality;

--analyze review_score by observing product information
select order_id, 
review_score, 
price, 
product_category_translated_name, 
product_photos_qty, 
product_weight_g, 
product_size_cm3
from #olist_service_quality_review


--4.1 preference: product and payment
--product selecting (details about product characteristics), 
--count (order_id), count (customer_id)
select top 10 * from olist_customers_dataset;
select top 10 * from olist_geolocation_dataset;
select top 10 * from product_category_name_translation;
--change column name
sp_rename 'product_category_name_translation.column2', 'product_category_translated_name', 'COLUMN';

select top 10 * from olist_products_dataset;

select top 10 * from olist_order_items_dataset;

create table #olist_preference_product_payment 
(order_id nvarchar(100), 
product_id nvarchar(100), 
price float, 
freight_value float, 
product_category_translated_name nvarchar(50), 
payment_value float, 
payment_type nvarchar(50));

insert into #olist_preference_product_payment
select a.order_id, 
a.product_id, 
price, 
freight_value, 
product_category_translated_name, 
payment_value, 
payment_type 
from 
(select order_id, product_id, price, freight_value 
from olist_order_items_dataset) a 
left join 
(select product_id, product_category_name 
from olist_products_dataset) b 
on a.product_id = b.product_id 
left join
product_category_name_translation c 
on b.product_category_name = c.product_category_name
left join 
(select order_id,
payment_value, 
payment_type  
from olist_order_payments_dataset) d 
on a.order_id = d.order_id;

select * from #olist_preference_product_payment;

--champion product by value and num of order 
select product_category_translated_name, 
count (order_id) num_order,
round(sum(payment_value),2) total_value
from #olist_preference_product_payment
group by product_category_translated_name
order by num_order desc, total_value desc

--favorite payment method
select payment_type, 
count (order_id) num_order
from #olist_preference_product_payment
group by payment_type
order by num_order desc;

--4. preference: location 
with olist_preference_location as
(select a.customer_id, 
customer_city,
customer_state,
seller_city,
seller_state
from
    (select order_id, customer_id 
    from olist_orders_dataset) a 
left join 
    (select customer_id, 
    customer_city,
    customer_state
    from olist_customers_dataset) b 
on a.customer_id = b.customer_id
left join 
(select order_id, 
seller_id 
from olist_order_items_dataset) d 
on a.order_id = d.order_id 
left join 
(select seller_id, 
seller_city, 
seller_state 
from olist_sellers_dataset) e 
on d.seller_id = e.seller_id) 
select count(*) from olist_preference_location;
