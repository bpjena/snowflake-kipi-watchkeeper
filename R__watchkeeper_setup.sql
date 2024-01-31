----------------------------CREATION OF AUDIT OBJECTS AND SETTING THE WORKSHEET CONTEXT----------------------
USE ROLE AUDIT;

USE DATABASE LESL_AUDIT_DB;

USE WAREHOUSE LESL_AUDIT_WH;

CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_AUDIT;

--#CREATE WAREHOUSE IF NOT EXISTS LESL_AUDIT_WH LIKE LESL_AUDIT_DEV_UAT_WH;

--------------------------------1. Storage monitor tables------------------------

-----------// CREATE Stored Procedure--------------------
CREATE OR REPLACE PROCEDURE Storage_Monitor_SP()  
RETURNS VARCHAR  
LANGUAGE JAVASCRIPT  
AS  
$$  

var orgCommand1="CREATE TRANSIENT TABLE if not exists ORG_STORAGE as SELECT USAGE_DATE,SERVICE_TYPE, AVERAGE_BYTES/(1024*1024*1024*1024)  as AVERAGE_TB, ACCOUNT_NAME, REGION FROM SNOWFLAKE.ORGANIZATION_USAGE.PREVIEW_STORAGE_DAILY_HISTORY;"

var orgCommand2="Truncate table ORG_STORAGE;"

var orgCommand3="Insert into ORG_STORAGE SELECT USAGE_DATE,SERVICE_TYPE, AVERAGE_BYTES/(1024*1024*1024*1024)  as AVERAGE_TB,ACCOUNT_NAME,REGION FROM SNOWFLAKE.ORGANIZATION_USAGE.PREVIEW_STORAGE_DAILY_HISTORY;"



var accCommand1="CREATE  TRANSIENT TABLE if not exists ACCOUNT_STORAGE as select USAGE_DATE,STORAGE_BYTES/(1024*1024*1024*1024) AS STORAGE_TB ,STAGE_BYTES/(1024*1024*1024*1024) AS STAGE_TB,FAILSAFE_BYTES/(1024*1024*1024*1024) AS FAILSAFE_TB from SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE;"

var accCommand2="Truncate table ACCOUNT_STORAGE;"

var accCommand3="Insert into ACCOUNT_STORAGE select USAGE_DATE,STORAGE_BYTES/(1024*1024*1024*1024) AS STORAGE_TB ,STAGE_BYTES/(1024*1024*1024*1024) AS STAGE_TB,FAILSAFE_BYTES/(1024*1024*1024*1024) AS FAILSAFE_TB from SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE;"



var dbCommand1="CREATE TRANSIENT TABLE if not exists DB_STORAGE as select DATABASE_NAME,USAGE_DATE,AVERAGE_DATABASE_BYTES/(1024*1024*1024*1024) AS AVERAGE_DATABASE_TB ,AVERAGE_FAILSAFE_BYTES/(1024*1024*1024*1024) AS AVERAGE_FAILSAFE_TB from SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY;"

var dbCommand2="Truncate table DB_STORAGE;"

var dbCommand3="Insert into DB_STORAGE select DATABASE_NAME,USAGE_DATE,AVERAGE_DATABASE_BYTES/(1024*1024*1024*1024) AS AVERAGE_DATABASE_TB ,AVERAGE_FAILSAFE_BYTES/(1024*1024*1024*1024) AS AVERAGE_FAILSAFE_TB from SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY;"



var tabCommand1="CREATE TRANSIENT TABLE IF NOT EXISTS TABLE_AND_SCHEMA_STORAGE as select TABLE_CREATED,TABLE_NAME, TABLE_SCHEMA, SCHEMA_CREATED AS SCHEMA_CREATION_DATE, TABLE_CATALOG AS TABLE_DATABASE, DELETED AS IS_TABLE_DELETED, IS_TRANSIENT, ACTIVE_BYTES/(1024*1024*1024) AS ACTIVE_GB,TIME_TRAVEL_BYTES/(1024*1024*1024) AS TIME_TRAVEL_GB,FAILSAFE_BYTES/(1024*1024*1024) AS FAILSAFE_GB,RETAINED_FOR_CLONE_BYTES/(1024*1024*1024) AS RETAINED_FOR_CLONE_GB from SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;"

var tabCommand2="Truncate table TABLE_AND_SCHEMA_STORAGE;"

var tabCommand3="Insert into TABLE_AND_SCHEMA_STORAGE select TABLE_CREATED,TABLE_NAME, TABLE_SCHEMA, SCHEMA_CREATED AS SCHEMA_CREATION_DATE, TABLE_CATALOG AS TABLE_DATABASE, DELETED AS IS_TABLE_DELETED, IS_TRANSIENT, ACTIVE_BYTES/(1024*1024*1024) AS ACTIVE_GB,TIME_TRAVEL_BYTES/(1024*1024*1024) AS TIME_TRAVEL_GB,FAILSAFE_BYTES/(1024*1024*1024) AS FAILSAFE_GB,RETAINED_FOR_CLONE_BYTES/(1024*1024*1024) AS RETAINED_FOR_CLONE_GB from SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;"



var orgCmd_dict1 = {sqlText: orgCommand1};
var orgCmd_dict2 = {sqlText: orgCommand2};
var orgCmd_dict3 = {sqlText: orgCommand3};

var accCmd_dict1 = {sqlText: accCommand1};
var accCmd_dict2 = {sqlText: accCommand2};
var accCmd_dict3 = {sqlText: accCommand3};

var dbCmd_dict1 = {sqlText: dbCommand1};
var dbCmd_dict2 = {sqlText: dbCommand2};
var dbCmd_dict3 = {sqlText: dbCommand3};

var tabCmd_dict1 = {sqlText: tabCommand1};
var tabCmd_dict2 = {sqlText: tabCommand2};
var tabCmd_dict3 = {sqlText: tabCommand3};


var org_stmt1 = snowflake.createStatement(orgCmd_dict1);
var org_rs1 = org_stmt1.execute();

var org_stmt2 = snowflake.createStatement(orgCmd_dict2);
var org_rs2 = org_stmt2.execute();

var org_stmt3 = snowflake.createStatement(orgCmd_dict3);
var org_rs3 = org_stmt3.execute();

var acc_stmt1 = snowflake.createStatement(accCmd_dict1);
var acc_rs1 = acc_stmt1.execute();

var acc_stmt2 = snowflake.createStatement(accCmd_dict2);
var acc_rs2 = acc_stmt2.execute();

var acc_stmt3 = snowflake.createStatement(accCmd_dict3);
var acc_rs3 = acc_stmt3.execute();

var db_stmt1 = snowflake.createStatement(dbCmd_dict1);  
var db_rs1 = db_stmt1.execute();

var db_stmt2 = snowflake.createStatement(dbCmd_dict2);  
var db_rs2 = db_stmt2.execute();

var db_stmt3 = snowflake.createStatement(dbCmd_dict3);  
var db_rs3 = db_stmt3.execute();


var tab_stmt1 = snowflake.createStatement(tabCmd_dict1);  
var tab_rs1 = tab_stmt1.execute();

var tab_stmt2 = snowflake.createStatement(tabCmd_dict2);  
var tab_rs2 = tab_stmt2.execute();

var tab_stmt3 = snowflake.createStatement(tabCmd_dict3);  
var tab_rs3 = tab_stmt3.execute();

-----------//CREATE TASK : CHANGE CRON EXPRESSION TO SCHEDULE SP-----------
snowflake.execute({sqlText:`CREATE OR REPLACE TASK Monitor_Storage_Monitor_Task WAREHOUSE = 'LESL_AUDIT_WH' SCHEDULE = 'USING CRON 0 2 * * * UTC'  AS call Storage_Monitor_SP();`});
snowflake.execute({sqlText:`alter task Monitor_Storage_Monitor_Task resume;`});


return 'TRANSIENT TABLES CREATED FOR STORAGE COST MONITORING. TABLES NAME :  1.ORG_STORAGE_COST   2.ACCOUNT_STORAGE_COST   3.DB_STORAGE_COST   4.TABLE_STORAGE_COST';  

$$;

call Storage_Monitor_SP();

--TRANSIENT TABLES CREATED FOR STORAGE COST MONITORING. TABLES NAME :  1.ORG_STORAGE_COST   2.ACCOUNT_STORAGE_COST   3.DB_STORAGE_COST   4.TABLE_STORAGE_COST


----------------------------2. Compute monitor queries----------------------------

create or replace TRANSIENT TABLE TABLE_ROLE_MONITOR (
	DATE DATETIME,
	ROLE_NAME VARCHAR(16777216),
	USER_NAME VARCHAR(16777216),
	WAREHOUSE_NAME VARCHAR(16777216),
	STMT_CNT NUMBER(18,0),
	ESTIMATED_CREDITS NUMBER(38,12));


    create or replace TRANSIENT TABLE TABLE_USER_MONITOR (
	DATE DATETIME,
	USER_NAME VARCHAR(16777216),
	STMT_CNT NUMBER(18,0),
	ESTIMATED_CREDITS NUMBER(38,12));


    create or replace TRANSIENT TABLE TABLE_WAREHOUSE_MONITOR (
	WAREHOUSE_NAME VARCHAR(16777216),
	START_DATE DATETIME,
	DATE DATE,
	DAILY_CREDITS_USED NUMBER(38,9),
	DAILY_CREDITS_USED_COMPUTE NUMBER(38,9),
	DAILY_CREDITS_USED_CLOUD NUMBER(38,9)
);

create or replace TRANSIENT TABLE TABLE_PIPE_MONITOR (
	DATE DATETIME,
	PIPE_NAME VARCHAR(16777216),
	DAILY_CREDITS_USED NUMBER(38,9)
);


create or replace TRANSIENT TABLE TABLE_SERVERLESSTASK_MONITOR (
	DATE DATETIME,
	TASK_NAME VARCHAR(16777216),
	DAILY_CREDITS_USED NUMBER(38,9)
);


create or replace TRANSIENT TABLE TABLE_REPLICATION_MONITOR (
	DATE DATETIME,
	DATABASE_NAME VARCHAR(16777216),
	DAILY_CREDITS_USED NUMBER(38,9)
);


create or replace TRANSIENT TABLE TABLE_AUTOMATICCLUSTERING_MONITOR (
	DATE DATETIME,
	DATABASE_NAME VARCHAR(16777216),
    	TABLE_NAME VARCHAR(16777216),
	DAILY_CREDITS_USED NUMBER(38,9)
);

create or replace TRANSIENT TABLE TABLE_MATERIALIZEDVIEWREFRESH_MONITOR (
	DATE DATETIME,
	DATABASE_NAME VARCHAR(16777216),
   	TABLE_NAME VARCHAR(16777216),
	DAILY_CREDITS_USED NUMBER(38,9)
);

create or replace TRANSIENT TABLE TABLE_SEARCHOPTIMIZATION_MONITOR (
	DATE DATETIME,
	DATABASE_NAME VARCHAR(16777216),
    	TABLE_NAME VARCHAR(16777216),
	DAILY_CREDITS_USED NUMBER(38,9)
);


insert into TABLE_USER_MONITOR (DATE ,
	USER_NAME ,
	STMT_CNT ,
	ESTIMATED_CREDITS)
Select convert_timezone('UTC', START_TIME)::datetime as date,
user_name,
count(*) as stmt_cnt,			
sum(execution_time/1000 *			
case warehouse_size			
when 'X-Small' then 1/60/60			
when 'Small'   then 2/60/60			
when 'Medium'  then 4/60/60			
when 'Large'   then 8/60/60			
when 'X-Large' then 16/60/60			
when '2X-Large' then 32/60/60			
when '3X-Large' then 64/60/60			
when '4X-Large' then 128/60/60			
else 0				
end) as estimated_credits
from snowflake.account_usage.query_history
group by 1,2
order by 1 desc,4 desc,2 ;

insert into TABLE_ROLE_MONITOR(DATE ,ROLE_NAME ,USER_NAME ,WAREHOUSE_NAME ,STMT_CNT ,ESTIMATED_CREDITS)
select convert_timezone('UTC', start_time)::datetime as date,role_name,user_name,warehouse_name ,count(*) as stmt_cnt,			
sum(execution_time/1000 *			
case warehouse_size			
when 'X-Small' then 1/60/60			
when 'Small'   then 2/60/60			
when 'Medium'  then 4/60/60			
when 'Large'   then 8/60/60			
when 'X-Large' then 16/60/60			
when '2X-Large' then 32/60/60			
when '3X-Large' then 64/60/60			
when '4X-Large' then 128/60/60			
else 0			
end) as estimated_credits
from snowflake.account_usage.query_history  
group by 1,2,3,4			
order by 1 desc,4 desc,2;


insert into TABLE_WAREHOUSE_MONITOR
(WAREHOUSE_NAME,START_DATE,DATE,DAILY_CREDITS_USED, DAILY_CREDITS_USED_COMPUTE, DAILY_CREDITS_USED_CLOUD)
SELECT WAREHOUSE_NAME, 
START_TIME as START_DATE,
convert_timezone('UTC', a.start_time)::datetime as date, 
SUM(CREDITS_USED) AS DAILY_CREDITS_USED, 
SUM(CREDITS_USED_COMPUTE) AS DAILY_CREDITS_USED_COMPUTE,
SUM(CREDITS_USED_CLOUD_SERVICES) AS DAILY_CREDITS_USED_CLOUD
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY AS a 
GROUP BY a.WAREHOUSE_NAME, a.START_TIME;



insert into  TABLE_PIPE_MONITOR(DATE, PIPE_NAME, DAILY_CREDITS_USED) 
select convert_timezone('UTC', start_time)::datetime as date,
pipe_name,
sum(credits_used) as DAILY_CREDITS_USED
from "SNOWFLAKE"."ACCOUNT_USAGE"."PIPE_USAGE_HISTORY"
group by 1,2
order by 1 desc,3 desc,2;



insert into  "TABLE_SERVERLESSTASK_MONITOR"(DATE, TASK_NAME, DAILY_CREDITS_USED) 
select convert_timezone('UTC', start_time)::datetime as date,
task_name,
sum(credits_used) as DAILY_CREDITS_USED
from "SNOWFLAKE"."ACCOUNT_USAGE"."SERVERLESS_TASK_HISTORY" 
group by 1,2
order by 1 desc,3 desc,2;



insert into  "TABLE_REPLICATION_MONITOR"(DATE,  DATABASE_NAME, DAILY_CREDITS_USED) 
select convert_timezone('UTC', start_time)::datetime as date,
database_name,
sum(credits_used) as DAILY_CREDITS_USED
from "SNOWFLAKE"."ACCOUNT_USAGE"."REPLICATION_USAGE_HISTORY"
group by 1,2
order by 1 desc,3 desc,2;



insert into  "TABLE_MATERIALIZEDVIEWREFRESH_MONITOR"(DATE, DATABASE_NAME,TABLE_NAME, DAILY_CREDITS_USED) 
select convert_timezone('UTC', start_time)::datetime as date,
database_name,
table_name,
sum(credits_used) as DAILY_CREDITS_USED
from "SNOWFLAKE"."ACCOUNT_USAGE"."MATERIALIZED_VIEW_REFRESH_HISTORY" 
group by 1,2,3
order by 1 desc,3 desc,2;


insert into  "TABLE_AUTOMATICCLUSTERING_MONITOR"(DATE, DATABASE_NAME,TABLE_NAME, DAILY_CREDITS_USED) 
select convert_timezone('UTC', start_time)::datetime as date,
database_name,
table_name,
sum(credits_used) as DAILY_CREDITS_USED
from "SNOWFLAKE"."ACCOUNT_USAGE"."AUTOMATIC_CLUSTERING_HISTORY" 
group by 1,2,3
order by 1 desc,3 desc,2;


insert into  "TABLE_SEARCHOPTIMIZATION_MONITOR"(DATE, DATABASE_NAME,TABLE_NAME, DAILY_CREDITS_USED) 
select convert_timezone('UTC', start_time)::datetime as date,
database_name,
table_name,
sum(credits_used) as DAILY_CREDITS_USED
from "SNOWFLAKE"."ACCOUNT_USAGE"."SEARCH_OPTIMIZATION_HISTORY" 
group by 1,2,3
order by 1 desc,3 desc,2;

-----------------------------------Stored procs for compute tables---------------------
CREATE OR REPLACE PROCEDURE SP_USER_MONITOR()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

snowflake.execute( {sqlText: `
delete from TABLE_USER_MONITOR 
where date = (Select max(date) 
from TABLE_USER_MONITOR);`} );
 
var sql_command = `
insert into TABLE_USER_MONITOR (DATE ,
	USER_NAME ,
	STMT_CNT ,
	ESTIMATED_CREDITS)
Select convert_timezone('UTC', START_TIME)::datetime as date,
user_name,
count(*) as stmt_cnt,			
sum(execution_time/1000 *			
case warehouse_size			
when 'X-Small' then 1/60/60			
when 'Small'   then 2/60/60			
when 'Medium'  then 4/60/60			
when 'Large'   then 8/60/60			
when 'X-Large' then 16/60/60			
when '2X-Large' then 32/60/60			
when '3X-Large' then 64/60/60			
when '4X-Large' then 128/60/60			
else 0				
end) as estimated_credits
from snowflake.account_usage.query_history 
WHERE convert_timezone('UTC', DATE) > (select max(convert_timezone('UTC', DATE)) from table_user_monitor)
group by 1,2
order by 1 desc,4 desc,2;`;
try {
   snowflake.execute({sqlText: sql_command});
   return "Success";
}
catch (err) {
   return "Failed" + err;
}
$$
;

call SP_USER_MONITOR();

show warehouses;

--Task to call SP_USER_MONITOR()
create or replace task credit_user_task
  warehouse = 'LESL_AUDIT_WH'
  schedule = 'USING CRON 0 3 * * * UTC'
as
  call SP_USER_MONITOR();
ALTER TASK credit_user_task resume;

show tasks;


--Stored procedure to update custom table table_role_monitor
CREATE OR REPLACE PROCEDURE SP_ROLE_MONITOR()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

 snowflake.execute( {sqlText: `
 delete from TABLE_ROLE_MONITOR 
 where date = (Select max(date) 
 from TABLE_ROLE_MONITOR);`} );
 
var sql_command = `
insert into TABLE_ROLE_MONITOR(DATE ,ROLE_NAME ,USER_NAME ,WAREHOUSE_NAME ,STMT_CNT ,ESTIMATED_CREDITS)
select convert_timezone('UTC', start_time)::datetime as date,ROLE_NAME,user_name,warehouse_name ,count(*) as stmt_cnt,			
sum(execution_time/1000 *			
case warehouse_size			
when 'X-Small' then 1/60/60			
when 'Small'   then 2/60/60			
when 'Medium'  then 4/60/60			
when 'Large'   then 8/60/60			
when 'X-Large' then 16/60/60			
when '2X-Large' then 32/60/60			
when '3X-Large' then 64/60/60			
when '4X-Large' then 128/60/60			
else 0			
end) as estimated_credits
from snowflake.account_usage.query_history
WHERE convert_timezone('UTC', DATE) > (select max(convert_timezone('UTC', DATE)) from table_role_monitor)
group by 1,2,3,4			
order by 1 desc,4 desc,2;`;
try {
   snowflake.execute({sqlText: sql_command});
   return "Success";
}
catch (err) {
   return "Failed" + err;
}
$$
;


call SP_ROLE_MONITOR();

show warehouses;

--Task to call SP_ROLE_MONITOR()
create or replace task credit_role_task
  warehouse = 'LESL_AUDIT_WH'
  schedule = 'USING CRON 0 3 * * * UTC'
as
  call SP_ROLE_MONITOR();
ALTER TASK credit_role_task resume;



CREATE OR REPLACE PROCEDURE SP_WAREHOUSE_MONITOR()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

snowflake.execute( {sqlText: `
 delete from TABLE_WAREHOUSE_MONITOR 
 where start_date = (Select max(start_date) 
 from TABLE_WAREHOUSE_MONITOR);`} );
 
var sql_command = `
insert into TABLE_WAREHOUSE_MONITOR
(WAREHOUSE_NAME,START_DATE,DATE,DAILY_CREDITS_USED, DAILY_CREDITS_USED_COMPUTE, DAILY_CREDITS_USED_CLOUD)
SELECT WAREHOUSE_NAME, 
START_TIME as START_DATE,
convert_timezone('UTC', a.start_time)::datetime as date, 
SUM(CREDITS_USED) AS DAILY_CREDITS_USED, 
SUM(CREDITS_USED_COMPUTE) AS DAILY_CREDITS_USED_COMPUTE,
SUM(CREDITS_USED_CLOUD_SERVICES) AS DAILY_CREDITS_USED_CLOUD
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY AS a 
WHERE convert_timezone('UTC', start_date) > (select max(convert_timezone('UTC', start_date)) from table_warehouse_monitor)
GROUP BY a.WAREHOUSE_NAME, a.START_TIME;`;

try {
   snowflake.execute({sqlText: sql_command});
   return "Success";
}
catch (err) {
   return "Failed" + err;
}
$$
;


call SP_WAREHOUSE_MONITOR();

show warehouses;

--Task to call SP_WAREHOUSE_MONITOR()
create or replace task credit_warehouse_task
  warehouse = 'LESL_AUDIT_WH'
  schedule = 'USING CRON 0 3 * * * UTC'
as
  call SP_WAREHOUSE_MONITOR();
ALTER TASK credit_warehouse_task resume;



--Stored procedure to update custom table table_pipe_monitor
CREATE OR REPLACE PROCEDURE SP_PIPE_MONITOR()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

snowflake.execute( {sqlText: `
 delete from TABLE_PIPE_MONITOR 
 where date = (Select max(date) 
 from TABLE_PIPE_MONITOR);`} );

var sql_command = `
insert into TABLE_PIPE_MONITOR(DATE, PIPE_NAME, DAILY_CREDITS_USED) 
select convert_timezone('UTC', start_time)::datetime as date,
pipe_name,
sum(credits_used) as DAILY_CREDITS_USED
from "SNOWFLAKE"."ACCOUNT_USAGE"."PIPE_USAGE_HISTORY" 
WHERE convert_timezone('UTC', DATE) > (select max(convert_timezone('UTC', DATE)) from table_pipe_monitor)
group by 1,2
order by 1 desc,3 desc,2;
`;

try {
   snowflake.execute({sqlText: sql_command});
   return "Success";
}
catch (err) {
   return "Failed" + err;
}
$$
;


call SP_PIPE_MONITOR();

show warehouses;

--Task to call SP_PIPE_MONITOR()
create or replace task credit_pipe_task
  warehouse = 'LESL_AUDIT_WH'
  schedule = 'USING CRON 0 3 * * * UTC'
as
  call SP_PIPE_MONITOR();
ALTER TASK credit_pipe_task resume;


--Stored procedure to update custom table table_serverlesstask_monitor

CREATE OR REPLACE PROCEDURE SP_STASK_MONITOR()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

snowflake.execute( {sqlText: `
 delete from TABLE_SERVERLESSTASK_MONITOR 
 where date = (Select max(date) 
 from TABLE_SERVERLESSTASK_MONITOR);`} );
 
var sql_command = `
insert into "TABLE_SERVERLESSTASK_MONITOR"(DATE, TASK_NAME, DAILY_CREDITS_USED) 
select convert_timezone('UTC', start_time)::datetime as date,
task_name,
sum(credits_used) as DAILY_CREDITS_USED
from "SNOWFLAKE"."ACCOUNT_USAGE"."SERVERLESS_TASK_HISTORY"
WHERE convert_timezone('UTC', DATE) > (select max(convert_timezone('UTC', DATE)) from table_serverlesstask_monitor)
group by 1,2
order by 1 desc,3 desc,2;
`;

try {
   snowflake.execute({sqlText: sql_command});
   return "Success";
}
catch (err) {
   return "Failed" + err;
}
$$
;

call SP_STASK_MONITOR();

show warehouses;


--Task to call SP_STASK_MONITOR()
create or replace task credit_stask_task
  warehouse = 'LESL_AUDIT_WH'
  schedule = 'USING CRON 0 3 * * * UTC'
as
  call SP_STASK_MONITOR();
ALTER TASK credit_stask_task resume;


--Stored procedure to update custom table_replication_monitor

CREATE OR REPLACE PROCEDURE SP_REPLICATION_MONITOR()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

snowflake.execute( {sqlText: `
 delete from TABLE_REPLICATION_MONITOR 
 where date = (Select max(date) 
 from TABLE_REPLICATION_MONITOR);`} );

var sql_command = `
insert into "TABLE_REPLICATION_MONITOR"(DATE,  DATABASE_NAME, DAILY_CREDITS_USED) 
select convert_timezone('UTC', start_time)::datetime as date,
database_name,
sum(credits_used) as DAILY_CREDITS_USED
from "SNOWFLAKE"."ACCOUNT_USAGE"."REPLICATION_USAGE_HISTORY" 
WHERE convert_timezone('UTC', DATE) > (select max(convert_timezone('UTC', DATE)) from table_replication_monitor)
group by 1,2
order by 1 desc,3 desc,2;
`;

try {
   snowflake.execute({sqlText: sql_command});
   return "Success";
}
catch (err) {
   return "Failed" + err;
}
$$;


call SP_REPLICATION_MONITOR();

show warehouses;

--Task to call SP_REPLICATION_MONITOR();
create or replace task credit_replication_task
  warehouse = 'LESL_AUDIT_WH'
  schedule = 'USING CRON 0 3 * * * UTC'
as
  call SP_REPLICATION_MONITOR();
ALTER TASK credit_replication_task resume;



--Stored procedure to update custom table table_materializedviewrefresh_monitor
CREATE OR REPLACE PROCEDURE SP_MVIEW_MONITOR()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

snowflake.execute( {sqlText: `
 delete from TABLE_MATERIALIZEDVIEWREFRESH_MONITOR 
 where date = (Select max(date) 
 from TABLE_MATERIALIZEDVIEWREFRESH_MONITOR);`} );

var sql_command = `
insert into "TABLE_MATERIALIZEDVIEWREFRESH_MONITOR"(DATE, DATABASE_NAME,TABLE_NAME, DAILY_CREDITS_USED) 
select convert_timezone('UTC', start_time)::datetime as date,
database_name,
table_name,
sum(credits_used) as DAILY_CREDITS_USED
from "SNOWFLAKE"."ACCOUNT_USAGE"."MATERIALIZED_VIEW_REFRESH_HISTORY"
WHERE convert_timezone('UTC', DATE) > (select max(convert_timezone('UTC', DATE)) from table_materializedviewrefresh_monitor)
group by 1,2,3
order by 1 desc,3 desc,2;
`;

try {
   snowflake.execute({sqlText: sql_command});
   return "Success";
}
catch (err) {
   return "Failed" + err;
}
$$
;


call SP_MVIEW_MONITOR();

show warehouses;

--Task to call SP_MVIEW_MONITOR()
create or replace task credit_mview_task
  warehouse = 'LESL_AUDIT_WH'
  schedule = 'USING CRON 0 3 * * * UTC'
as
  call SP_MVIEW_MONITOR(); 
ALTER TASK credit_mview_task resume;


--Stored procedure to update custom table table_automaticclustering_monitor

CREATE OR REPLACE PROCEDURE SP_AUTOCLUSTER_MONITOR()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

snowflake.execute( {sqlText: `
 delete from TABLE_AUTOMATICCLUSTERING_MONITOR 
 where date = (Select max(date) 
 from TABLE_AUTOMATICCLUSTERING_MONITOR);`} );

var sql_command = `
insert into "TABLE_AUTOMATICCLUSTERING_MONITOR"(DATE, DATABASE_NAME,TABLE_NAME, DAILY_CREDITS_USED) 
select convert_timezone('UTC', start_time)::datetime as date,
database_name,
table_name,
sum(credits_used) as DAILY_CREDITS_USED
from "SNOWFLAKE"."ACCOUNT_USAGE"."AUTOMATIC_CLUSTERING_HISTORY"
WHERE convert_timezone('UTC', DATE) > (select max(convert_timezone('UTC', DATE)) from table_automaticclustering_monitor)
group by 1,2,3
order by 1 desc,3 desc,2;
`;

try {
   snowflake.execute({sqlText: sql_command});
   return "Success";
}
catch (err) {
   return "Failed" + err;
}
$$
;


call SP_AUTOCLUSTER_MONITOR();

show warehouses;

--Task to call SP_autocluster_MONITOR()
create or replace task credit_autocluster_task
  warehouse = 'LESL_AUDIT_WH'
  schedule = 'USING CRON 0 3 * * * UTC'
as
  call SP_AUTOCLUSTER_MONITOR(); 
ALTER TASK credit_autocluster_task resume;


--Stored procedure to update custom table table_searchoptimization_monitor
CREATE OR REPLACE PROCEDURE SP_SEARCHOPTIMIZATION_MONITOR()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

snowflake.execute( {sqlText: `
 delete from TABLE_SEARCHOPTIMIZATION_MONITOR 
 where date = (Select max(date) 
 from TABLE_SEARCHOPTIMIZATION_MONITOR);`} );

var sql_command = `
insert into "TABLE_SEARCHOPTIMIZATION_MONITOR"(DATE, DATABASE_NAME,TABLE_NAME, DAILY_CREDITS_USED) 
select convert_timezone('UTC', start_time)::datetime as date,
database_name,
table_name,
sum(credits_used) as DAILY_CREDITS_USED
from "SNOWFLAKE"."ACCOUNT_USAGE"."SEARCH_OPTIMIZATION_HISTORY"
WHERE convert_timezone('UTC', DATE) > (select max(convert_timezone('UTC', DATE)) from table_searchoptimization_monitor)
group by 1,2,3
order by 1 desc,3 desc,2;
`;

try {
   snowflake.execute({sqlText: sql_command});
   return "Success";
}
catch (err) {
   return "Failed" + err;
}
$$
;


call SP_SEARCHOPTIMIZATION_MONITOR();

show warehouses;

--Task to call SP_SEARCHOPTIMIZATION_MONITOR()
create or replace task credit_SEARCHOPTIMIZATION_task
  warehouse = 'LESL_AUDIT_WH'
  schedule = 'USING CRON 0 3 * * * UTC'
as
  call SP_SEARCHOPTIMIZATION_MONITOR();
ALTER TASK credit_SEARCHOPTIMIZATION_task resume;



------------------------3. Performance monitor - table creation--------------------------

CREATE OR REPLACE TABLE timeout_ref (
  wh_size VARCHAR(16777216),
  timeout_1 NUMBER(38,0),
  timeout_2 NUMBER(38,0),
  priority_1 array,
  priority_2 array
);


INSERT INTO timeout_ref(wh_size, TIMEOUT_1,TIMEOUT_2) 
VALUES 
    ('X-Small',20,16),
    ('Small',18,14),
    ('Medium',15,12),
    ('Large',12,10),
    ('X-Large',12,10),
    ('2X-Large',10,8),
    ('3X-Large',8,6),
    ('4X-Large',8,6),
    ('5X-Large',5,3),
    ('6X-Large',5,3);

UPDATE "TIMEOUT_REF" SET PRIORITY_1 = (select array_construct('ACCOUNTADMIN','SYSADMIN','SECURITYADMIN'));

UPDATE "TIMEOUT_REF" SET PRIORITY_2 = (select array_construct('USERADMIN','PUBLIC'));

create or replace table Long_query(QUERY_ID string,QUERY_TEXT string,START_TIME datetime,END_TIME datetime,DATABASE_NAME varchar,SCHEMA_NAME varchar,WAREHOUSE_NAME varchar,WAREHOUSE_SIZE varchar,USER_NAME varchar,ROLE_NAME varchar,TOTAL_ELAPSED_TIME float,EXECUTION_STATUS varchar,Performance varchar);


insert into LONG_QUERY
select QUERY_ID,QUERY_TEXT,START_TIME,END_TIME,DATABASE_NAME,SCHEMA_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,USER_NAME,ROLE_NAME,
case when START_TIME<END_TIME then total_elapsed_time*1.667/100000 
     when START_TIME>END_TIME then timediff(minute,START_TIME,current_timestamp()) end as timediff,EXECUTION_STATUS,
case when array_contains(ROLE_NAME::VARIANT,PRIORITY_1) and timediff>=TIMEOUT_1 or 
          array_contains(ROLE_NAME::VARIANT,PRIORITY_2) and timediff>=TIMEOUT_2 or
          (NOT(array_contains(ROLE_NAME::VARIANT,PRIORITY_1)) AND NOT(array_contains(ROLE_NAME::VARIANT,PRIORITY_2)) and timediff>=TIMEOUT_2)
           then 'LONG' 
     when array_contains(ROLE_NAME::VARIANT,PRIORITY_1) and timediff<TIMEOUT_1 or
          array_contains(ROLE_NAME::VARIANT,PRIORITY_2) and timediff<TIMEOUT_2 or          
          (NOT(array_contains(ROLE_NAME::VARIANT,PRIORITY_1)) AND NOT(array_contains(ROLE_NAME::VARIANT,PRIORITY_2)) and timediff<TIMEOUT_2) or
          WAREHOUSE_SIZE is null then 'SHORT' 
    end as Performance 
from (select * from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY) a
left join "TIMEOUT_REF" b on a.WAREHOUSE_SIZE = b.WH_SIZE;


---------------------SP to update long query-------------------------------
create or replace procedure insert_long_query()
returns string language javascript execute as caller as
$$

snowflake.execute({sqlText: `
delete from LONG_QUERY 
where START_TIME = (Select max(START_TIME) 
from LONG_QUERY);`});


snowflake.execute({sqlText:
`insert into LONG_QUERY
select QUERY_ID,QUERY_TEXT,START_TIME,END_TIME,DATABASE_NAME,SCHEMA_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,USER_NAME,ROLE_NAME,
case when START_TIME<END_TIME then total_elapsed_time*1.667/100000 
     when START_TIME>END_TIME then timediff(minute,START_TIME,current_timestamp()) end as timediff,EXECUTION_STATUS,
case when array_contains(ROLE_NAME::VARIANT,PRIORITY_1) and timediff>=TIMEOUT_1 or 
          array_contains(ROLE_NAME::VARIANT,PRIORITY_2) and timediff>=TIMEOUT_2 or
          (NOT(array_contains(ROLE_NAME::VARIANT,PRIORITY_1)) AND NOT(array_contains(ROLE_NAME::VARIANT,PRIORITY_2)) and timediff>=TIMEOUT_2)
           then 'LONG' 
     when array_contains(ROLE_NAME::VARIANT,PRIORITY_1) and timediff<TIMEOUT_1 or
          array_contains(ROLE_NAME::VARIANT,PRIORITY_2) and timediff<TIMEOUT_2 or          
          (NOT(array_contains(ROLE_NAME::VARIANT,PRIORITY_1)) AND NOT(array_contains(ROLE_NAME::VARIANT,PRIORITY_2)) and timediff<TIMEOUT_2) or
          WAREHOUSE_SIZE is null then 'SHORT' 
    end as Performance 
from (select * from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY) a
left join "TIMEOUT_REF" b on a.WAREHOUSE_SIZE = b.WH_SIZE
where START_TIME > (select max(start_time) from LONG_QUERY)`
});
return 'INSERTED'
$$;



call insert_long_query();

show warehouses;

create or replace task call_insert_long_query
warehouse = 'LESL_AUDIT_WH'
schedule = 'USING CRON 0 3 * * * UTC'
As
call insert_long_query();
ALTER TASK call_insert_long_query resume; 



--------------------------4. Security monitoring - creation of tables-----------------------
CREATE
OR REPLACE TRANSIENT TABLE ROLES_INFO_TB (
    CREATED_ON TIMESTAMP_LTZ,
    NAME VARCHAR,
    IS_DEFAULT VARCHAR,
    IS_CURRENT VARCHAR,
    IS_INHERITED VARCHAR,
    ASSIGNED_TO_USERS NUMBER,
    GRANTED_TO_ROLES NUMBER,
    GRANTED_ROLES NUMBER,
    OWNER VARCHAR,
    RCOMMENT VARCHAR,
    REFRESH_DATE TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    ROLE_TYPE varchar(30)
) COMMENT = 'stores snapshot of current snowflake roles';


CREATE
OR REPLACE TABLE network_policies_tb(
    CREATED_ON TIMESTAMP_LTZ,
    NAME VARCHAR,
    COMMENT varchar(100),
    ENTRIES_IN_ALLOWED_IP_LIST NUMBER,
    ENTRIES_IN_BLOCKED_IP_LIST NUMBER,
    CURRENT_TIMESTAMPS TIMESTAMP_LTZ
) COMMENT = 'STORES NETWORK POLICIES INCLUDING BLOCKED AND ALLOWED IP';

CREATE OR REPLACE TRANSIENT TABLE Authentication_Breakdown_TB ( 
Event_Count NUMBER(10),
Authentication_Factor Varchar(30),
Second_Authentication_Factor Varchar(30)
);

-------------------------------SPs to update above tables---------------------

CREATE OR REPLACE PROCEDURE SNAPSHOT_ROLES()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = 'Captures the snapshot of roles and inserts the records into ROLES_INFO_TB'
EXECUTE AS CALLER
AS
$$
var result = "SUCCESS";
try {
	snowflake.execute( {sqlText: "alter table ROLES_INFO_TB drop column ROLE_TYPE;"} );
    snowflake.execute( {sqlText: "truncate table ROLES_INFO_TB;"} );
    snowflake.execute( {sqlText: "show roles;"} );
    var dcroles_tbl_sql = `insert into ROLES_INFO_TB select *,CURRENT_TIMESTAMP() from table(result_scan(last_query_id()));`;
    snowflake.execute( {sqlText: dcroles_tbl_sql} );
    snowflake.execute( {sqlText: "alter table ROLES_INFO_TB add column ROLE_TYPE varchar(30);"} );
    snowflake.execute( {sqlText: "update ROLES_INFO_TB set ROLE_TYPE = 'Disconnected Role' where Granted_to_roles = 0 and Granted_roles = 0;"} );
    snowflake.execute( {sqlText: "update ROLES_INFO_TB set ROLE_TYPE = 'Connected Role' where Granted_to_roles > 0 or Granted_roles > 0;"} );
}
    
catch (err) {
    result = "FAILED: Code: " + err.code + "\n State: " + err.state;result += "\n Message: " + err.message;result += "\nStack Trace:\n" + err.stackTraceTxt;
}
    return result;
$$;


call SNAPSHOT_ROLES();

show warehouses;

create or replace task ROLES_INFO_TB_TASK
warehouse = 'LESL_AUDIT_WH'
schedule = 'USING CRON 0 3 * * * UTC'
As
call SNAPSHOT_ROLES();

ALTER TASK ROLES_INFO_TB_TASK resume;



--------------------------------
-------------------User needs to have ACCOUNTADMIN ACCESS to execute this----------------------
/*CREATE OR REPLACE PROCEDURE network_pol_sp()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = 'Captures the snapshot of policies and inserts the records into network_policies_tb'
EXECUTE AS CALLER
AS
$$
var result = "SUCCESS";
try {
    snowflake.execute( {sqlText: "truncate table network_policies_tb;"} );
    snowflake.execute( {sqlText: "use role ACCOUNTADMIN;"} );
    snowflake.execute( {sqlText: "show network policies;"} );
    var network_policies_tb_sql = `insert into network_policies_tb select *,CURRENT_TIMESTAMP() from table(result_scan(last_query_id()));`;
    snowflake.execute( {sqlText: network_policies_tb_sql} );
    snowflake.execute( {sqlText: `use role MONITOR_ADMIN;`} );
} 
catch (err) {
    result = "FAILED: Code: " + err.code + "\n State: " + err.state;result += "\n Message: " + err.message;result += "\nStack Trace:\n" + err.stackTraceTxt;
}
    return result;
$$;

call network_pol_sp();

show warehouses;

create or replace task call_insert_network_policies_tb_task
warehouse = 'LESL_AUDIT_WH'
schedule = 'USING CRON 0 3 * * * UTC'
As
call network_pol_sp();

ALTER TASK call_insert_network_policies_tb_task resume;*/
--------------------------------------------------------------


CREATE OR REPLACE PROCEDURE Authentication_breakdown_sp()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
COMMENT = 'Create a new custom table having authentication factors and its event count'
EXECUTE AS CALLER
AS
$$
var result = "SUCCESS";
try {

snowflake.execute( {sqlText: `
truncate table AUTHENTICATION_BREAKDOWN_TB;`} );

    snowflake.execute( {sqlText: `
Insert into
    AUTHENTICATION_BREAKDOWN_TB (
        Event_Count,
        Authentication_Factor,
        Second_Authentication_Factor
    )
SELECT
    count(First_Authentication_Factor) as Event_Count,
    First_Authentication_Factor as Authentication_Factor,
    Second_Authentication_Factor
from
    SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
group by
    First_Authentication_Factor,
    Second_Authentication_Factor;
`} );

    snowflake.execute( {sqlText: `
update
    AUTHENTICATION_BREAKDOWN_TB
set
    AUTHENTICATION_FACTOR = (
        SELECT
            CONCAT(
                AUTHENTICATION_FACTOR,
                CONCAT('_', second_authentication_factor)
            )
        from
            AUTHENTICATION_BREAKDOWN_TB
        where
            second_authentication_factor IS NOT NULL
    )
where
    second_authentication_factor is not null;
`} );
    
    }
    
catch (err) {
    result = "FAILED: Code: " + err.code + "\n State: " + err.state;result += "\n Message: " + err.message;result += "\nStack Trace:\n" + err.stackTraceTxt;
}
    return result;
$$;

call Authentication_breakdown_sp();

show warehouses;

create or replace task Authentication_breakdown_task
warehouse = 'LESL_AUDIT_WH'
schedule = 'USING CRON 0 3 * * * UTC'
As
call Authentication_breakdown_sp();

ALTER TASK Authentication_breakdown_task resume;

-------------------------------------------------------------END-------------------------------------------------------------
