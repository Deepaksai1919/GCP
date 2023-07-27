-- How many questions each tag has

select tag, count(distinct id) n_questions from `jrjames83-1171.sampledata.top_questions` 
group by 1
order by 2 desc