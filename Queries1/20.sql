-- How many customers are acquired each year and month

with first_order as (
    SELECT customer_unique_id, 
        extract(year from o.order_purchase_timestamp) as year, 
        extract(month from o.order_purchase_timestamp) as month
    FROM `jrjames83-1171.sampledata.orders` o join `jrjames83-1171.sampledata.customers` c
    on o.customer_id = c.customer_id
    qualify row_number() over(
        partition by customer_unique_id order by order_purchase_timestamp
    ) = 1
)

select year, month, count(customer_unique_id) as total_customers_acquired from first_order
group by 1,2
order by 1,2;

-- Find the average time in between the orders for 
-- customers who have placed more than one order

with base_table as (
    -- Getting records with row number
    SELECT customer_unique_id, 
        o.order_purchase_timestamp,
        row_number() over(
          partition by customer_unique_id order by order_purchase_timestamp
        ) as rn
    FROM `jrjames83-1171.sampledata.orders` o join `jrjames83-1171.sampledata.customers` c
    on o.customer_id = c.customer_id
),
single_orders as (
    -- Customers with only one purchase
  select customer_unique_id from base_table group by 1 having max(rn) = 1
),
previous_orders as (
    -- using lag to find the previous order timestamp
    select customer_unique_id, order_purchase_timestamp, 
    lag(order_purchase_timestamp) over(
        partition by customer_unique_id order by order_purchase_timestamp
    ) as previous_order_timestamp, rn from base_table where customer_unique_id not in 
    (select customer_unique_id from single_orders)
    order by customer_unique_id, rn
),
difference_view as (
    -- Calculating number of days between consecutive orders
    select *, 
    date_diff(order_purchase_timestamp, previous_order_timestamp, day) as days_difference 
    from previous_orders
)

select customer_unique_id, avg(days_difference) as avg_days from difference_view group by 1



