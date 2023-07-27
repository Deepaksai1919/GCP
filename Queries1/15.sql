-- lag, ||

-- Growth of programming language YoY
with language_view as (SELECT distinct id, quarter, quarter_views,
case
  when title like '%python%' then 'python'
  when title like '%javascript%' then 'javascript'
  when title like '%java%' then 'java'
  when title like '%ruby%' then 'ruby'
  when title like '%php%' then 'php'
  else 'other'
end as language
FROM `jrjames83-1171.sampledata.top_questions`),

yearly_language_view as (
select language, extract(year from quarter) as year, sum(quarter_views) as yearly_views from language_view
group by 1,2
order by 1,2
)

-- select * from yearly_language_view

-- language	    year	yearly_views
-- java	        2017	23503583
-- java	        2018	29552573
-- java	        2019	30539028
-- java	        2020	14790056
-- javascript	  2017	7953931
-- javascript	  2018	10010934
-- javascript	  2019	11256396
-- javascript	  2020	6037424
-- other	      2017	1611524931
-- other	      2018	2205820634
-- other	      2019	2391894600
-- other	      2020	1229243125
-- php	        2017	8339191
-- php	        2018	11293088
-- php	        2019	11776354
-- php	        2020	5604613
-- python	      2017	19952439
-- python	      2018	32174249
-- python	      2019	41033853
-- python	      2020	23236975
-- ruby	        2017	819280
-- ruby	        2018	947295
-- ruby	        2019	1007032
-- ruby	        2020	510369

-- select *, 
-- lag(yearly_views) over(partition by language order by year) as previous_year_views 
-- from yearly_language_view

-- language	    year	yearly_views	previous_year_views
-- other	    2017	1611524931	    null
-- other	    2018	2205820634	    1611524931
-- other	    2019	2391894600	    2205820634
-- other	    2020	1229243125	    2391894600
-- python	    2017	19952439	    null
-- python	    2018	32174249	    19952439
-- python	    2019	41033853	    32174249
-- python	    2020	23236975	    41033853
-- javascript	2017	7953931	        null
-- javascript	2018	10010934	    7953931
-- javascript	2019	11256396	    10010934
-- javascript	2020	6037424	        11256396
-- java	        2017	23503583	    null
-- java	        2018	29552573	    23503583
-- java	        2019	30539028	    29552573
-- java	        2020	14790056	    30539028
-- ruby	        2017	819280	        null
-- ruby	        2018	947295	        819280
-- ruby	        2019	1007032	        947295
-- ruby	        2020	510369	        1007032
-- php	        2017	8339191	        null
-- php	        2018	11293088	    8339191
-- php	        2019	11776354	    11293088
-- php	        2020	5604613	        11776354

growth_view as (
select *, lag(yearly_views) over(partition by language order by year) as previous_year_views
from yearly_language_view)

select language, year, 
round(coalesce((yearly_views/previous_year_views - 1), 0) * 100, 2) || '%'  as YoY_growth 
from growth_view;

-- language	  year	YoY_growth
-- other	      2017	0%
-- other	      2018	36.88%
-- other	      2019	8.44%
-- other	      2020	-48.61%
-- java	      2017	0%
-- java	      2018	25.74%
-- java	      2019	3.34%
-- java	      2020	-51.57%
-- php	        2017	0%
-- php	        2018	35.42%
-- php	        2019	4.28%
-- php	        2020	-52.41%
-- python	    2017	0%
-- python	    2018	61.25%
-- python	    2019	27.54%
-- python	    2020	-43.37%
-- javascript	2017	0%
-- javascript	2018	25.86%
-- javascript	2019	12.44%
-- javascript	2020	-46.36%
-- ruby	      2017	0%
-- ruby	      2018	15.63%
-- ruby	      2019	6.31%
-- ruby	      2020	-49.32%