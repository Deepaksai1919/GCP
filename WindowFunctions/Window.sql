-- Analytic functions

WITH Produce AS
 (SELECT 'kale' as item, 23 as purchases, 'vegetable' as category
  UNION ALL SELECT 'banana', 2, 'fruit'
  UNION ALL SELECT 'cabbage', 9, 'vegetable'
  UNION ALL SELECT 'apple', 8, 'fruit'
  UNION ALL SELECT 'leek', 2, 'vegetable'
  UNION ALL SELECT 'lettuce', 10, 'vegetable')
SELECT * FROM Produce

/*-------------------------------------*
 | item      | category   | purchases  |
 +-------------------------------------+
 | kale      | vegetable  | 23         |
 | banana    | fruit      | 2          |
 | cabbage   | vegetable  | 9          |
 | apple     | fruit      | 8          |
 | leek      | vegetable  | 2          |
 | lettuce   | vegetable  | 10         |
 *-------------------------------------*/

WITH Employees AS
 (SELECT 'Isabella' as name, 2 as department, DATE(1997, 09, 28) as start_date
  UNION ALL SELECT 'Anthony', 1, DATE(1995, 11, 29)
  UNION ALL SELECT 'Daniel', 2, DATE(2004, 06, 24)
  UNION ALL SELECT 'Andrew', 1, DATE(1999, 01, 23)
  UNION ALL SELECT 'Jacob', 1, DATE(1990, 07, 11)
  UNION ALL SELECT 'Jose', 2, DATE(2013, 03, 17))
SELECT * FROM Employees

/*-------------------------------------*
 | name      | department | start_date |
 +-------------------------------------+
 | Isabella  | 2          | 1997-09-28 |
 | Anthony   | 1          | 1995-11-29 |
 | Daniel    | 2          | 2004-06-24 |
 | Andrew    | 1          | 1999-01-23 |
 | Jacob     | 1          | 1990-07-11 |
 | Jose      | 2          | 2013-03-17 |
 *-------------------------------------*/

WITH Farm AS
 (SELECT 'cat' as animal, 23 as population, 'mammal' as category
  UNION ALL SELECT 'duck', 3, 'bird'
  UNION ALL SELECT 'dog', 2, 'mammal'
  UNION ALL SELECT 'goose', 1, 'bird'
  UNION ALL SELECT 'ox', 2, 'mammal'
  UNION ALL SELECT 'goat', 2, 'mammal')
SELECT * FROM Farm

/*-------------------------------------*
 | animal    | category   | population |
 +-------------------------------------+
 | cat       | mammal     | 23         |
 | duck      | bird       | 3          |
 | dog       | mammal     | 2          |
 | goose     | bird       | 1          |
 | ox        | mammal     | 2          |
 | goat      | mammal     | 2          |
 *-------------------------------------*/

-- Q1:  Compute grand total for all items in Produce table

SELECT item, purchases, category, 
sum(purchases) over() as grand_total  FROM `bigquery-392209.window_dataset.Produce`
/*
| item    | purchases | category  | grand_total |
|---------|-----------|-----------|-------------|
| banana  | 2         | fruit     | 54          |
| kale    | 23        | vegetable | 54          |
| apple   | 8         | fruit     | 54          |
| cabbage | 9         | vegetable | 54          |
| lettuce | 10        | vegetable | 54          |
| leek    | 2         | vegetable | 54          |*/

-- Q2:  Compute subtotal for each category in Product table

SELECT item, purchases, category, 
sum(purchases) over(partition by category) as sub_total  
FROM `bigquery-392209.window_dataset.Produce` order by category, purchases

/*
| item    | purchases | category  | sub_total |
|---------|-----------|-----------|-----------|
| banana  | 2         | fruit     | 10        |
| apple   | 8         | fruit     | 10        |
| leek    | 2         | vegetable | 44        |
| cabbage | 9         | vegetable | 44        |
| lettuce | 10        | vegetable | 44        |
| kale    | 23        | vegetable | 44        |*/

-- Q3:  Compute cumulative sum for each category in Produce table

-- Sum is computed wrt to the order defined in order clause of window

SELECT
  item,
  purchases,
  category,
  SUM(purchases) OVER(
    PARTITION BY category ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_sum
FROM
  `bigquery-392209.window_dataset.Produce`

/*
| item    | purchases | category  | cumulative_sum |
|---------|-----------|-----------|----------------|
| banana  | 2         | fruit     | 2              |
| apple   | 8         | fruit     | 10             |
| kale    | 23        | vegetable | 23             |
| lettuce | 10        | vegetable | 33             |
| leek    | 2         | vegetable | 35             |
| cabbage | 9         | vegetable | 44             |*/

SELECT
  item,
  purchases,
  category,
  SUM(purchases) OVER(
    PARTITION BY category ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_sum
FROM
  `bigquery-392209.window_dataset.Produce`
order by category, purchases

-- Rows are same as that of the above query but display of result is sorted
/*
| item    | purchases | category  | cumulative_sum |
|---------|-----------|-----------|----------------|
| banana  | 2         | fruit     | 2              |
| apple   | 8         | fruit     | 10             |
| leek    | 2         | vegetable | 35             |
| cabbage | 9         | vegetable | 44             |
| lettuce | 10        | vegetable | 33             |
| kale    | 23        | vegetable | 23             |*/

SELECT
  item,
  purchases,
  category,
  SUM(purchases) OVER(
    PARTITION BY category order by purchases ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_sum
FROM
  `bigquery-392209.window_dataset.Produce`

-- Here purchases are sorted and similarly cumulative sum is also 
-- calculated on the sorted data because of `order by purchases`

/*
| item    | purchases | category  | cumulative_sum |
|---------|-----------|-----------|----------------|
| banana  | 2         | fruit     | 2              |
| apple   | 8         | fruit     | 10             |
| leek    | 2         | vegetable | 2              |
| cabbage | 9         | vegetable | 11             |
| lettuce | 10        | vegetable | 21             |
| kale    | 23        | vegetable | 44             |*/


SELECT
  item,
  purchases,
  category,
  SUM(purchases) OVER(order by purchases ROWS BETWEEN UNBOUNDED PRECEDING AND current row) AS cumulative_sum,
  SUM(purchases) OVER(order by purchases ROWS BETWEEN UNBOUNDED PRECEDING AND 2 preceding) AS shift_cumulative_sum
FROM
  `bigquery-392209.window_dataset.Produce`
  order by purchases

/*
| item    | purchases | category  | cumulative_sum | shift_cumulative_sum |
|---------|-----------|-----------|----------------|----------------------|
| banana  | 2         | fruit     | 2              | null                 |
| leek    | 2         | vegetable | 4              | null                 |
| apple   | 8         | fruit     | 12             | 2                    |
| cabbage | 9         | vegetable | 21             | 4                    |
| lettuce | 10        | vegetable | 31             | 12                   |
| kale    | 23        | vegetable | 54             | 21                   |*/

-- Q4:  Compute moving average of Purchases from Produce table

SELECT
  item,
  purchases,
  category,
  SUM(purchases) OVER(ORDER BY purchases ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_sum,
  COUNT(purchases) OVER(ORDER BY purchases ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_count,
  round(AVG(purchases) OVER(ORDER BY purchases ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), 2) AS moving_average
FROM
  `bigquery-392209.window_dataset.Produce`
ORDER BY
  purchases

-- Moving average calculated for row above till row below
/*
| item    | purchases | category  | moving_sum | moving_count | moving_average |
|---------|-----------|-----------|------------|--------------|----------------|
| banana  | 2         | fruit     | 4          | 2            | 2.0            |
| leek    | 2         | vegetable | 12         | 3            | 4.0            |
| apple   | 8         | fruit     | 19         | 3            | 6.33           |
| cabbage | 9         | vegetable | 27         | 3            | 9.0            |
| lettuce | 10        | vegetable | 42         | 3            | 14.0           |
| kale    | 23        | vegetable | 33         | 2            | 16.5           |*/


-- Q5:  Compute the number of animals that have a similar population count from the Farm table
-- Eg:  There are 2 dogs. So the number required is the count of all the animals which are
--  1 to 3 in number

SELECT
  *,
  generate_array(population-1, population+1) as population_range_considered,
  COUNT(1) OVER(ORDER BY population RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS similar_population,
  ARRAY_AGG(animal) OVER(ORDER BY population RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS similar_population_animals
FROM
  `bigquery-392209.window_dataset.Farm`
ORDER BY
  population
/*
| animal | population | category | population_range_considered | similar_population | similar_population_animals |
|--------|------------|----------|-----------------------------|--------------------|----------------------------|
| goose  | 1          | bird     | "[0,1,2]"                   | 4                  | "[goose,dog,goat,ox]"      |
| dog    | 2          | mammal   | "[1,2,3]"                   | 5                  | "[goose,dog,goat,ox,duck]" |
| goat   | 2          | mammal   | "[1,2,3]"                   | 5                  | "[goose,dog,goat,ox,duck]" |
| ox     | 2          | mammal   | "[1,2,3]"                   | 5                  | "[goose,dog,goat,ox,duck]" |
| duck   | 3          | bird     | "[2,3,4]"                   | 4                  | "[dog,goat,ox,duck]"       |
| cat    | 23         | mammal   | "[22,23,24]"                | 1                  | "[cat]"                    |
*/

-- Q6: Calculate the most populated animal in each category from Farm table

SELECT
  *,
  FIRST_VALUE(animal) OVER(PARTITION BY category ORDER BY population DESC) AS most_populated_animal,
  FIRST_VALUE(animal) OVER(PARTITION BY category ORDER BY population, animal DESC) AS least_populated_animal_1,
  FIRST_VALUE(animal) OVER(PARTITION BY category ORDER BY population, animal) AS least_populated_animal_2
FROM
  `bigquery-392209.window_dataset.Farm`
ORDER BY
  category,
  population

/*
| animal | population | category | most_populated_animal | least_populated_animal_1 | least_populated_animal_2 |
|--------|------------|----------|-----------------------|--------------------------|--------------------------|
| goose  | 1          | bird     | duck                  | goose                    | goose                    |
| duck   | 3          | bird     | duck                  | goose                    | goose                    |
| dog    | 2          | mammal   | cat                   | ox                       | dog                      |
| goat   | 2          | mammal   | cat                   | ox                       | dog                      |
| ox     | 2          | mammal   | cat                   | ox                       | dog                      |
| cat    | 23         | mammal   | cat                   | ox                       | dog                      |
*/

-- Q7:  Get the most popular item for each cateogry in Produce table

SELECT
  *,
  FIRST_VALUE(item) OVER(PARTITION BY category ORDER BY purchases rows between unbounded preceding and unbounded following) AS least_sold_item,
  last_value(item) over(partition by category order by purchases desc rows between unbounded preceding and unbounded following) as least_sold_item_1,
  FIRST_VALUE(item) OVER(PARTITION BY category ORDER BY purchases desc rows between unbounded preceding and unbounded following) AS most_sold_item,
  last_value(item) over(partition by category order by purchases rows between unbounded preceding and unbounded following) as most_sold_item_1
FROM
  `bigquery-392209.window_dataset.Produce`
ORDER BY
  category,
  purchases
/*
| item    | purchases | category  | least_sold_item | least_sold_item_1 | most_sold_item | most_sold_item_1 |
|---------|-----------|-----------|-----------------|-------------------|----------------|------------------|
| banana  | 2         | fruit     | banana          | banana            | apple          | apple            |
| apple   | 8         | fruit     | banana          | banana            | apple          | apple            |
| leek    | 2         | vegetable | leek            | leek              | kale           | kale             |
| cabbage | 9         | vegetable | leek            | leek              | kale           | kale             |
| lettuce | 10        | vegetable | leek            | leek              | kale           | kale             |
| kale    | 23        | vegetable | leek            | leek              | kale           | kale             |
*/


SELECT
  *,
  FIRST_VALUE(item) OVER(window_spec) AS least_sold_item,
  LAST_VALUE(item) OVER(window_spec) AS most_sold_item
FROM
  `bigquery-392209.window_dataset.Produce`
WINDOW
  window_spec AS (
  PARTITION BY
    category
  ORDER BY
    purchases ROWS BETWEEN UNBOUNDED PRECEDING
    AND UNBOUNDED FOLLOWING)
ORDER BY
  category,
  purchases

-- Above example uses Named Window

/*
| item    | purchases | category  | least_sold_item | most_sold_item |
|---------|-----------|-----------|-----------------|----------------|
| banana  | 2         | fruit     | banana          | apple          |
| apple   | 8         | fruit     | banana          | apple          |
| leek    | 2         | vegetable | leek            | kale           |
| cabbage | 9         | vegetable | leek            | kale           |
| lettuce | 10        | vegetable | leek            | kale           |
| kale    | 23        | vegetable | leek            | kale           |
*/

SELECT
  *,
  FIRST_VALUE(item) OVER(window_spec1) AS least_sold_item,
  LAST_VALUE(item) OVER(window_spec2) AS least_sold_item_1,
  FIRST_VALUE(item) OVER(window_spec2) AS most_sold_item,
  LAST_VALUE(item) OVER(window_spec1) AS most_sold_item_1
FROM
  `bigquery-392209.window_dataset.Produce`
WINDOW
  window_spec1 AS (
  PARTITION BY
    category
  ORDER BY
    purchases ROWS BETWEEN UNBOUNDED PRECEDING
    AND UNBOUNDED FOLLOWING),
  window_spec2 AS (
  PARTITION BY
    category
  ORDER BY
    purchases DESC ROWS BETWEEN UNBOUNDED PRECEDING
    AND UNBOUNDED FOLLOWING)
ORDER BY
  category,
  purchases

/*
| item    | purchases | category  | least_sold_item | least_sold_item_1 | most_sold_item | most_sold_item_1 |
|---------|-----------|-----------|-----------------|-------------------|----------------|------------------|
| banana  | 2         | fruit     | banana          | banana            | apple          | apple            |
| apple   | 8         | fruit     | banana          | banana            | apple          | apple            |
| leek    | 2         | vegetable | leek            | leek              | kale           | kale             |
| cabbage | 9         | vegetable | leek            | leek              | kale           | kale             |
| lettuce | 10        | vegetable | leek            | leek              | kale           | kale             |
| kale    | 23        | vegetable | leek            | leek              | kale           | kale             |
*/

SELECT
  fruit,
  ANY_VALUE(fruit) OVER (ORDER BY LENGTH(fruit) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS any_value,
  ARRAY_AGG(fruit) OVER (ORDER BY LENGTH(fruit) ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS agg
FROM UNNEST(["apple", "banana", "pear"]) as fruit;

/*
| fruit  | any_value | agg              |
|--------|-----------|------------------|
| pear   | pear      | "[pear]"         |
| apple  | apple     | "[pear,apple]"   |
| banana | banana    | "[apple,banana]" |*/

