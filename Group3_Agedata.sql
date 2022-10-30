--- AWS S3 Configuration.
--creating storage integration

create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::743853371262:role/sprint3'
  storage_allowed_locations = ('s3://group3sprint3/');

DESC INTEGRATION s3_int;

--creating file format
create or replace file format sprint_3.public.my_csv_format
type = csv field_delimiter = ',' skip_header = 1 null_if = ('NULL', 'null') empty_field_as_null = true;

desc file format my_csv_format;

--creation of stage
create or replace stage my_s3_stage
  storage_integration = s3_int
  url = 's3://group3sprint3/'
  file_format = my_csv_format;
  
  

--creating table
create or replace table agedata(
id string,
name string,
short_description string,
gender string,
country string,
occupation string, 
birth_year string,
death_year string,
manner_of_death string,
age_of_death string
);
drop table agedata;
list @my_s3_stage;


--loading data to table from stage
copy into agedata from @my_s3_stage
file_format = (type=csv field_optionally_enclosed_by='"' )
--files = ('agedataset01.csv', 'agedataset02.csv', 'agedataset03.csv','agedataset04.csv', 'agedataset05.csv' )
pattern='.*.csv'
on_error='skip_file';



--creating task to schedule job at 12 AM IST hours on every thursday
CREATE TASK mytask_hour
  WAREHOUSE = sfwarehouse
  SCHEDULE = 'USING CRON 0 0 * * THU Asia/Kolkata'
  TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
AS
copy into agedata
from @my_s3_stage
file_format = (type = csv field_optionally_enclosed_by = '"')
pattern = '.*.csv'
on_error='skip_file';

-- Check sheduled tasks.
show tasks;

-- Put task in the shedule.
alter task mytask_hour resume;
alter task mytask_hour suspend;


--craeting a table 
create table agedata_snowpipe as select *from agedata where 1=2;

-- Creating Snowpipe
create or replace pipe sprint3_snowpipe auto_ingest=true as
    copy into agedata
    from @my_s3_stage;
    
desc pipe sprint3_snowpipe;
show stages;

select *from agedata_snowpipe;


--check status of snowpipe 
select SYSTEM$PIPE_STATUS('sprint3_snowpipe');

--refresh the pipe 
alter pipe sprint3_snowpipe refresh;

show pipes;


    
--creating a stream
create or replace stream agedata_check on table agedata;

select *from agedata_check;

select * from agedata;

create or replace table sprint3_target_t (id string, name string, short_description string, gender string, country string, occupation string, birth_year string, death_year string, manner_of_death string, age_of_death string, stream_type string default null, rec_version number default 0,REC_DATE TIMESTAMP_LTZ);

update agedata set name ='Robert downey' where id='Q23';


--resume task 
alter task mytask_hour resume;


merge into sprint3_target_t t
using agedata_check a 
on t.id=a.id and (metadata$action='DELETE')
when matched and metadata$isupdate='FALSE' then update set rec_version=9999, stream_type='DELETE'
when matched and metadata$isupdate='TRUE' then update set rec_version=rec_version-1,stream_type='UPDATE'
when not matched then insert  (id, name, short_description, gender, country, occupation, birth_year, death_year, manner_of_death, age_of_death,stream_type,rec_version,REC_DATE) values(a.id, a.name, a.short_description, a.gender, a.country, a.occupation, a.birth_year, a.death_year, a.manner_of_death, a.age_of_death, metadata$action,0,CURRENT_TIMESTAMP() );

--suspend task
alter task mytask_hour suspend;
   
--insert into agedata (id, name, short_description, gender, country, occupation, birth_year, death_year, manner_of_death, age_of_death) values ('Q3321542','John', 'Politician','Male', 'Germany', 'Politician', '1945', '1985','' ,'41');


--Row level security
create or replace role United_States_of_America;   
create or replace role Kingdom_of_France;

create or replace table agedata_roles( agedata_role_name varchar,
                                       agedata_role_alias varchar);
                                       
insert into agedata_roles values('United_States_of_America','United States of America'),
                           ('Kingdom_of_France','Kingdom of France');
                           
select *from agedata_roles;                                                
                                                
create or replace user "George Washington" password = 'temp123' default_Role = 'United_States_of_America';
grant role United_States_of_America to user "George Washington";

create or replace user "François Villon" password = 'temp123' default_Role = ' Kingdom_of_France';
grant role  Kingdom_of_France to user "François Villon";


select current_user();

grant role  United_States_of_America to user VIDHATHRIMN;
grant role  Kingdom_of_France to user VIDHATHRIMN;


create or replace secure view vw_age as
select a.*
from agedata a
where upper(a.country) in (select upper(agedata_role_alias)
               from agedata_roles 
               where upper(agedata_role_name) = upper(current_role()));
               
grant select on view vw_age to role United_States_of_America;
grant select on view vw_age to role Kingdom_of_France;
              


grant usage on warehouse sfwarehouse to role United_States_of_America;
grant usage on warehouse sfwarehouse to role Kingdom_of_France;


grant usage on database sprint_3 to role United_States_of_America;
grant usage on database sprint_3 to role Kingdom_of_France;


grant usage on schema public to role United_States_of_America;
grant usage on schema public to role Kingdom_of_France;


use role United_States_of_America;
use database sprint_3;
use schema public;
select * from vw_age;


--column level security
create or replace table mod_1 as select * from agedata;
 
--creating masking policy
CREATE MASKING POLICY sprint_3.PUBLIC.agedata_mask AS (VAL STRING) RETURNS STRING ->
      CASE
        WHEN CURRENT_ROLE() IN ('mod_agedata') THEN VAL
        ELSE '******'
      END;

--creating role
create role mod_agedata;
 
create or replace table mod_2(manner_of_death1 string masking policy agedata_mask, manner_of_death2 string);
 
insert into mod_2(manner_of_death1, manner_of_death2)
select manner_of_death, manner_of_death from agedata;
 
--grant permission to the role
GRANT SELECT ON sprint_3.PUBLIC.mod_2 TO ROLE mod_agedata;
    grant usage on warehouse sfwarehouse to role mod_agedata;
 
    grant usage on database sprint_3 to role mod_agedata;

    grant usage on schema public to role mod_agedata;

    grant role mod_agedata to user VIDHATHRIMN;

--fetch the result
select * from mod_2;