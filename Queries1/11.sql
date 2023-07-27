-- Get all the views (sum of views) for questions containing 'python' in their title

-- The table has granularity of tag and quarter so same question with same tag has multiple rows
-- If we sum the views, we are taking sum of duplicates as well

with distinct_question_views as (select id, quarter, quarter_views 
from `jrjames83-1171.sampledata.top_questions` 
where lower(title) like '%python%'
group by 1,2,3)

select sum(quarter_views) total_views from distinct_question_views;

-- OR

with distinct_question_views as (
    select distinct id, quarter, quarter_views 
    from `jrjames83-1171.sampledata.top_questions` where lower(title) like '%python%'
)

select sum(quarter_views) total_views from distinct_question_views;