-- CASE
-- COALESCE
-- IF
-- NULLIF
-- IFNULL

WITH Numbers AS (
  SELECT 90 as A, 2 as B UNION ALL
  SELECT 50, 8 UNION ALL
  SELECT 60, 6 UNION ALL
  SELECT 50, 10
)
SELECT
  A,
  B,
  CASE A
    WHEN 90 THEN 'red'
    WHEN 50 THEN 'blue'
    ELSE 'green'
    END
    AS result
FROM Numbers

/*------------------*
 | A  | B  | result |
 +------------------+
 | 90 | 2  | red    |
 | 50 | 8  | blue   |
 | 60 | 6  | green  |
 | 50 | 10 | blue   |
 *------------------*/


 WITH Numbers AS (
  SELECT 90 as A, 2 as B UNION ALL
  SELECT 50, 6 UNION ALL
  SELECT 20, 10
)
SELECT
  A,
  B,
  CASE
    WHEN A > 60 THEN 'red'
    WHEN B = 6 THEN 'blue'
    ELSE 'green'
    END
    AS result
FROM Numbers

/*------------------*
 | A  | B  | result |
 +------------------+
 | 90 | 2  | red    |
 | 50 | 6  | blue   |
 | 20 | 10 | green  |
 *------------------*/

SELECT COALESCE('A', 'B', 'C') as result

/*--------*
 | result |
 +--------+
 | A      |
 *--------*/

SELECT COALESCE(NULL, 'B', 'C') as result

/*--------*
 | result |
 +--------+
 | B      |
 *--------*/

-- IF(expr, true_result, else_result)

 WITH Numbers AS (
  SELECT 90 as A, 2 as B UNION ALL
  SELECT 50, 6 UNION ALL
  SELECT 20, 10
)
select *,
if(
    A>60, 'red', if(
                    B=6,'blue','green'
                )
) as result
from numbers;

/*------------------*
 | A  | B  | result |
 +------------------+
 | 90 | 2  | red    |
 | 50 | 6  | blue   |
 | 20 | 10 | green  |
 *------------------*/

--  IFNULL(a,b) is same as COALESCE(A,B)

/* NULLIF(expr, expr_to_match)

if expr = expr_to_match then returns null else expr

*/

SELECT NULLIF(0, 0) as result

/*--------*
 | result |
 +--------+
 | NULL   |
 *--------*/

SELECT NULLIF(10, 0) as result

/*--------*
 | result |
 +--------+
 | 10     |
 *--------*/