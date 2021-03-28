1. Create database :
	create database vin_emp_loc location '/user/cloudera/myhivedata';
2. show databases;
3. use vin_emp_loc;
4. show tables;
5. Create table:
	create table emp_global(id int,name string,city string,continent string)
	row format delimited
	fields terminated by ','
	stored as textfile ;
6.To check table structure:
	desc emp_global;
	describe emp_global;
7. loading data in the table:
	load data local inpath 'employee.txt' into table emp_global;
8. To check table type:
	describe extended emp_global;
9.How to load data in the hdfs:
	hadoop fs -put employee.csv empdata
10.To view hdfs data :  hadoop fs -cat empdata/employee.csv
11. create external table:
	create external table if not exists emp_global(Emp_id int,Last_name string,designation string,job_id int,hire_date date,base_salary int, commission string, increment_pct int)
	row format delimited
	fields terminated by ','
	stored as textfile 
	location '/user/cloudera/empdata';

12. Different file formate:
	1. Text/csv File.
	2. Sequence File.
	3. RC File.
	4. AVRO File.
	5. ORC File.
	6. Parquet File.
13. Create orc file formate tables:

create table if not exists emp_orc(Emp_id int,Last_name string,designation string,job_id int,hire_date string,base_salary int, commission string, increment_pct int)
row format delimited
fields terminated by ','
stored as orcfile ;

14. Inserting data from other table:
	insert into emp_orc Select * from emp;
	
15. Create a table whose schema is exactly like an existing table,
	create table emp_par LIKE emp_orc stored as parquetfile ;
	create table emp_avro LIKE emp_orc stored as avrofile ;

16. Loading data having multiple data in a single column.

create table if not exists sibling_data (
name string, age int, country string, siblings array<string> )
row format delimited
fields terminated by ','
collection items terminated by '#'
lines terminated by '\n'
stored as textfile ;

load data local inpath 'siblings.txt' into table sibling_data;

17. To create table with multiple inputs of different data type in a single column, 

create table auto_details(company string, model string, fuel string,
basic_specs struct<vehicle_type : string, doors : int, gears : int>,
engine_specs struct<cc : int, bhp : double>)
row format delimited
fields terminated by ','
collection items terminated by '#' ;

load data local inpath 'autodata.txt' into table auto_details;

18. Rename a table:
alter table auto_details rename to auto_table;
19. Rename a column:
alter table auto_table change fuel fuel_type string;

20. Add column:
alter table auto_table add columns(milage double);

21. delete columns:
alter table auto_table replace columns(company string, model string, fuel_type string,
basic_specs struct<vehicle_type : string, doors : int, gears : int>,
engine_specs struct<cc : int, bhp : double>);

22. Query on hive table;

create table if not exists company.empdata(Emp_id int,Last_name string,designation string,job_id int,hire_date string,base_salary int, commission double, increment_pct double)
row format delimited
fields terminated by ','
tblproperties('skip.header.line.count'='1');
--tblproperties('skip.fotter.line.count'='1');

23. creating a view 
	create view dep_mng as select * from empdata where designation = 'MANAGER';
	drop view dep_mng;
	desc formatted dep_mng;

24. creating partition in the hive:

create table empdata_part(Emp_id int,Last_name string,designation string,job_id int,hire_date string,base_salary int, commission double, increment_pct double)
partitioned by (continent string)
row format delimited
fields terminated by ','
stored as textfile ;


