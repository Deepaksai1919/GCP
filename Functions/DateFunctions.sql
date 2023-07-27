SELECT date_add(date '2023-07-11', interval 45 day);
-- 2023-08-25
SELECT date_diff(date '2023-08-24', date '2023-07-11', day)
-- 45
SELECT date_diff(date '2023-08-24', date '2023-07-11', week)
-- 6
SELECT DATE_SUB(DATE '2023-07-11', INTERVAL -5 DAY) AS five_days_ago;
-- 2023-07-16
SELECT DATE_SUB(DATE '2023-07-11', INTERVAL 5 week);
-- 2023-06-06
with date_cte as (select date '2023-07-11' as cur_date)
select cur_date, date_trunc(cur_date, week), date_trunc(cur_date, week(monday)) from date_cte;
/*
| cur_date   | f0_        | f1_        |
|------------|------------|------------|
| 2023-07-11 | 2023-07-09 | 2023-07-10 |*/

-- week starts from sunday so first date of week is the date for previous sunday.
-- It can be overridden by week(monday) so first day of the week is previous monday.


with date_cte as (select date '2023-07-11' as cur_date)
select extract(week from cur_date), 
extract(day from cur_date), 
extract(month from cur_date), 
extract(quarter from cur_date) from date_cte

/*
| f0_ | f1_ | f2_ | f3_ |
|-----|-----|-----|-----|
| 28  | 11  | 7   | 3   |*/

select format_date('%A', date '2023-07-11')
-- Tuesday
