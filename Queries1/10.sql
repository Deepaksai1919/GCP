-- Get all the questions associated with each tag

select tag, count(distinct title) n_questions, array_agg(distinct title) distinct_questions 
from `jrjames83-1171.sampledata.top_questions` 
group by 1;

-- Get all the views (sum of views) for questions containing 'python' in their title

select sum(total_views) total_views from `jrjames83-1171.sampledata.top_questions` 
where lower(title) like '%python%';