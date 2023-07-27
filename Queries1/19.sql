-- Which customer city is responsible for the most orders

SELECT c.customer_city, count(1) total_orders FROM `jrjames83-1171.sampledata.orders` o join 
`jrjames83-1171.sampledata.customers` c
on o.customer_id = c.customer_id
group by 1
order by 2 desc limit 1

-- OR

SELECT c.customer_city, count(1) total_orders FROM `jrjames83-1171.sampledata.orders` o join 
`jrjames83-1171.sampledata.customers` c
on o.customer_id = c.customer_id
group by 1
qualify rank() over(order by total_orders desc) = 1
