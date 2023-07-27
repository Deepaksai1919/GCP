-- Window functions
-- Date Time functions

-- Get the % records of each status of orders

with cte as (
    SELECT order_status, count(1) order_count  
    FROM `jrjames83-1171.sampledata.orders` group by 1
),
total_count as (
    select sum(order_count) as total_orders_count from cte
)
select order_status, round((order_count/total_orders_count) * 100, 2) || '%' as orders_pct 
from cte join total_count on 1 = 1;

-- OR

SELECT order_status, round((count(1) / sum(count(1)) over()) * 100, 2) || '%' as orders_pct
FROM `jrjames83-1171.sampledata.orders`
group by 1;

-- SELECT order_status, count(1) as order_count, sum(count(1)) over() as total_orders  
-- FROM `jrjames83-1171.sampledata.orders` group by 1 

-- order_status	order_count	total_orders
-- unavailable	609	        99441
-- approved	    2	        99441
-- invoiced	    314	        99441
-- canceled	    625	        99441
-- shipped	    1107	    99441
-- delivered	96478	    99441
-- created	    5	        99441
-- processing	301	        99441


SELECT order_status, 
count(1), 
sum(1), 
sum(count(1)) over(), 
sum(sum(1)) over(), 
count(sum(1)) over(), 
count(1) over(), 
sum(1) over()
FROM `jrjames83-1171.sampledata.orders`
group by 1;
-- order_status	count(1)    sum(1)  sum(count(1)) over()    sum(sum(1)) over()  count(sum(1)) over()   count(1) over() sum(1) over()
-- shipped	    1107	    1107	99441	                99441	            8	                    8	            8
-- canceled	    625	        625	    99441	                99441	            8	                    8	            8
-- invoiced	    314	        314	    99441	                99441	            8	                    8	            8
-- created	    5	        5	    99441	                99441	            8	                    8	            8
-- unavailable	609	        609	    99441	                99441	            8	                    8	            8
-- processing	301	        301	    99441	                99441	            8	                    8	            8
-- approved	    2	        2	    99441	                99441	            8	                    8	            8
-- delivered	96478	    96478	99441	                99441	            8	                    8	            8


select date_trunc('2023-07-10', year);
-- 2023-01-01

-- Get the number of shipped/created orders per each year per each month. 
-- Consider order purchase timestamp

SELECT extract(year from order_purchase_timestamp) year, extract(month from order_purchase_timestamp) month, count(1) as total_orders FROM `jrjames83-1171.sampledata.orders` where
order_status in ('shipped', 'created')
group by 1, 2
order by 1, 2;

select 
    date_trunc('2023-07-10', month), 
    extract(year from date('2023-07-10')),
    extract(hour from timestamp('2023-07-10T10:20:30')), 
    timestamp_trunc(timestamp('2023-07-10T10:20:30'), hour);


-- f0_	        f1_	    f2_	    f3_
-- 2023-07-01	2023	10	    2023-07-10 10:00:00.000000 UTC