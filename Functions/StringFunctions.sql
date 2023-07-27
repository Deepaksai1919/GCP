WITH items AS
  (SELECT 'xxxapplexxx' as item
  UNION ALL
  SELECT 'yyybananayyy' as item
  UNION ALL
  SELECT 'zzzorangezzz' as item
  UNION ALL
  SELECT 'xyzpearxyz' as item)

SELECT
  LTRIM(item, 'xyz') as example
FROM items;

/*-----------*
 | example   |
 +-----------+
 | applexxx  |
 | bananayyy |
 | orangezzz |
 | pearxyz   |
 *-----------*/

select repeat('ab',10)
-- abababababababababab

select split('Hello There', ' ')
/*
| f0_             |
|-----------------|
| "[Hello,There]" |*/

select strpos('abcd@12.com','@')
-- 5
-- 1 based position

WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  SUBSTR(item, 2, 2) as example
FROM items;

/*---------*
 | example |
 +---------+
 | pp      |
 | an      |
 | ra      |
 *---------*/

WITH items AS
  (SELECT 'apple' as item
  UNION ALL
  SELECT 'banana' as item
  UNION ALL
  SELECT 'orange' as item)

SELECT
  SUBSTR(item, -2) as example
FROM items;

/*---------*
 | example |
 +---------+
 | le      |
 | na      |
 | ge      |
 *---------*/