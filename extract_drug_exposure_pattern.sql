with 
seq as
(
select row_number() over (order by unnest(str)) as idx
      ,unnest(str) as str
  from (select string_to_array('a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t|u|v|w|x|y|z', '|') as str) t1
),
--환자별 처방
person_compare as 
(
select t1.person_id
      ,t1.drug_exposure_start_date as dt
      ,t1.drug_concept_id as concept_id
      ,lag(t1.drug_concept_id) over (partition by t1.person_id order by t1.drug_exposure_start_date) prev_concept_id
  from de.drug_exposure t1
group by t1.person_id
        ,dt
        ,concept_id
),
--환자별 distinct 처방
person_distinct as
(
select t1.person_id
      ,t1.concept_id
      ,row_number() over(partition by t1.person_id order by min(t1.dt)) as idx
  from person_compare t1
group by t1.person_id
        ,t1.concept_id
),
--환자/일자별 처방
person_dt as
(
select t1.person_id 
      ,t1.dt
      ,string_agg(t3.str, ',' order by t1.dt, t3.str) as arr
  from person_compare t1
  join person_distinct t2
   on  t1.person_id = t2.person_id
   and t1.concept_id = t2.concept_id
  join seq t3
   on  t2.idx = t3.idx
 where t1.prev_concept_id is null
    or t1.concept_id <> t1.prev_concept_id
group by t1.person_id 
        ,t1.dt
),
person_summary as
(
select t1.person_id
      ,string_agg(case when length(t1.arr) = 1 then t1.arr
			            else concat('(', t1.arr, ')')
			       end, '->') as pattern
  from person_dt t1
group by t1.person_id
)
select t1.pattern
      ,count(*) as cnt
  from person_summary t1
group by t1.pattern
order by cnt desc
limit 20