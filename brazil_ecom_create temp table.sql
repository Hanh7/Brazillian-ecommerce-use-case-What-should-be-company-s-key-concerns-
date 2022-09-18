--customer metrics
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

--service quality
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

--service quality 
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


--preference
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
