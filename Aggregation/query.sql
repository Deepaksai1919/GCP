WITH
  Precipitation AS (
    SELECT 2009 AS year, 'spring' AS season, 3 AS inches
    UNION ALL
    SELECT 2001, 'winter', 4
    UNION ALL
    SELECT 2003, 'fall', 1
    UNION ALL
    SELECT 2002, 'spring', 4
    UNION ALL
    SELECT 2005, 'summer', 1
  )
SELECT ANY_VALUE(year HAVING MAX inches) AS any_year_with_max_inches,
ANY_VALUE(year having min inches) as any_year_with_min_inches FROM Precipitation;
/*
| any_year_with_max_inches | any_year_with_min_inches |
|--------------------------|--------------------------|
| 2001                     | 2003                     |
*/

SELECT
  COUNT(*) AS total_count,
  count(1) as total_count_1,
  COUNT(fruit) AS non_null_count,
  MIN(fruit) AS min,
  MAX(fruit) AS max
FROM
  (
    SELECT NULL AS fruit
    UNION ALL
    SELECT 'apple' AS fruit
    UNION ALL
    SELECT 'pear' AS fruit
    UNION ALL
    SELECT 'orange' AS fruit
  )
/*
| total_count | total_count_1 | non_null_count | min   | max  |
|-------------|---------------|----------------|-------|------|
| 4           | 4             | 3              | apple | pear |
*/

SELECT ARRAY_AGG(x IGNORE NULLS) AS array_agg
FROM UNNEST([NULL, 1, -2, 3, -2, 1, NULL]) AS x;

/*-------------------*
 | array_agg         |
 +-------------------+
 | [1, -2, 3, -2, 1] |
 *-------------------*/

SELECT ARRAY_AGG(x ORDER BY ABS(x)) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

/*-------------------------*
 | array_agg               |
 +-------------------------+
 | [1, 1, 2, -2, -2, 2, 3] |
 *-------------------------*/

SELECT ARRAY_AGG(x LIMIT 5) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;

/*-------------------*
 | array_agg         |
 +-------------------+
 | [2, 1, -2, 3, -2] |
 *-------------------*/

SELECT ARRAY_AGG(x order by abs(x) LIMIT 5) AS array_agg
FROM UNNEST([2, 1, -2, 3, -2, 1, 2]) AS x;
/*
| array_agg       |
|-----------------|
| "[1,1,2,-2,-2]" |*/

SELECT STRING_AGG(fruit) AS string_agg, array_agg(fruit ignore nulls) as arr_agg
FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit;
-- string_agg ignores null values
/*
| string_agg               | arr_agg                    |
|--------------------------|----------------------------|
| "apple,pear,banana,pear" | "[apple,pear,banana,pear]" |*/

SELECT STRING_AGG(fruit,':') AS string_agg, array_agg(fruit ignore nulls) as arr_agg
FROM UNNEST(["apple", NULL, "pear", "banana", "pear"]) AS fruit;
/*
| string_agg             | arr_agg                    |
|------------------------|----------------------------|
| apple:pear:banana:pear | "[apple,pear,banana,pear]" |*/

SELECT STRING_AGG(DISTINCT fruit, " & " ORDER BY fruit DESC LIMIT 2) AS string_agg
FROM UNNEST(["apple", "pear", "banana", "pear"]) AS fruit;
/*---------------*
 | string_agg    |
 +---------------+
 | pear & banana |
 *---------------*/


 
