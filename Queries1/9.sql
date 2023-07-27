-- array_agg

-- Does a question have multiple tags?
select title, count(distinct tag) multiple_tags from `jrjames83-1171.sampledata.top_questions` 
group by 1
order by 2 desc;

select title, id, count(distinct tag) multiple_tags 
from `jrjames83-1171.sampledata.top_questions` 
group by 1,2
order by 3 desc;

-- What are those tags
select title, id, count(distinct tag) n_tags, array_agg(distinct tag) distinct_tags 
from `jrjames83-1171.sampledata.top_questions` 
group by 1,2
having n_tags > 1;