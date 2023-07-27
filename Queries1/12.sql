-- array_to_string, generate_array, cum-sum

-- Get all the tags comma separated for each id

SELECT id, array_to_string(array_agg(distinct tag), ',') 
FROM `jrjames83-1171.sampledata.top_questions` 
group by 1;

-- array_to_string will join all the elements of the array using the specified delimiter


with cte as (select num from unnest(generate_array(1, 100, 10)) as num)

select num, sum(num) over(rows between unbounded preceding and current row) from cte

-- num	f0_
-- 1	1
-- 11	12
-- 21	33
-- 31	64
-- 41	105
-- 51	156
-- 61	217
-- 71	288
-- 81	369
-- 91	460

