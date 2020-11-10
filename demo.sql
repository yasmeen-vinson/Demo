drop table if exists rising.myv_sync_query;
create table rising.myv_sync_query as (
with comp as ( 
select 
vanid
, (contactssurveyresponseid || vanid)::varchar(1024) as id
, surveyquestionname
, surveyresponsename
, datecanvassed
, contacttypeid
, initcap(vb_tsmart_first_name) as firstname
, initcap(vb_tsmart_last_name) as lastname
, initcap(vb_tsmart_full_address) as address
, vb_tsmart_zip as zip
, vb_tsmart_dob as dob
, coalesce(vb_voterbase_phone_wireless, vb_voterbase_phone) as phone
, statecode
from (
	select *
		, row_number() over (partition by vanid, surveyquestionname order by datecanvassed desc) as row
	from (
		select *
		from van.tsm_nextgen_contactssurveyresponses_vf csr
		left join van.tsm_nextgen_surveyquestions sq using(surveyquestionid)
		left join van.tsm_nextgen_surveyresponses sr using(surveyresponseid)
    left join ts.current_analytics ts on ts.vb_smartvan_id = csr.vanid and ts.vb_vf_source_state = csr.statecode
		where surveyquestionname in('2020 Partisan ID', 'Volunteer 2020')
		and surveyresponsename in ('Later', 'Maybe', 'Yes', '1 - Strong Democrat', '2 - Lean Democrat')
    and ts.vb_voterbase_age <= 35
		) 
	) where row = 1 
	--and csr.datecanvassed >= date('14-Sep-2020')
)

, log as (
select case when id ilike '%.0' then SUBSTRing(id, 1, LENGTH(id) - 2) else id end as id
  , questionname
  , return
  from rising.myv_sync_log
)
  
, vol as (
select * from comp c
left join log l using(id)
where surveyquestionname = 'Volunteer 2020'
and l.return is null
)

, partisan as (
select * from comp c
left join log l using(id)
where surveyquestionname = '2020 Partisan ID'
and l.return is null
)

select 
vanid
, coalesce(p.firstname, v.firstname) as firstname
, coalesce(p.lastname, v.lastname) as lastname
, coalesce(p.address, v.address) as address
, coalesce(p.zip, v.zip) as zip
, coalesce(p.dob, v.dob) as dob
, coalesce(p.phone, v.phone) as phone
, p.id as p_id
, v.id as v_id
, p.datecanvassed as p_date
, v.datecanvassed as v_date
, p.contacttypeid as p_contacttype
, v.contacttypeid as v_contacttype
, p.surveyresponsename as partisan
, v.surveyresponsename as vol
from partisan p
full join vol v using(vanid)
where coalesce(p.address, v.address) is not null
);

GRANT ALL ON table rising.myv_sync_query TO GROUP hq_data;
GRANT ALL ON table rising.myv_sync_query TO GROUP state_managers;
GRANT ALL ON table rising.myv_sync_log TO GROUP hq_data;
GRANT ALL ON table rising.myv_sync_log TO GROUP state_managers;
