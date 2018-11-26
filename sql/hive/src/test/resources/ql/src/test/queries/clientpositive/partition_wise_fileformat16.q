set hive.input.format = org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

-- This tests that the schema can be changed for binary serde data
create table partition_test_partitioned(key string, value string)
partitioned by (dt string) stored as textfile;
insert overwrite table partition_test_partitioned partition(dt='1')
select * from src where key = 238;

select * from partition_test_partitioned where dt is not null;
select key+key, value from partition_test_partitioned where dt is not null;

alter table partition_test_partitioned change key key int;

select key+key, value from partition_test_partitioned where dt is not null;
select * from partition_test_partitioned where dt is not null;

alter table partition_test_partitioned add columns (value2 string);

select key+key, value from partition_test_partitioned where dt is not null;
select * from partition_test_partitioned where dt is not null;

insert overwrite table partition_test_partitioned partition(dt='2')
select key, value, value from src where key = 86;

select key+key, value, value2, dt from partition_test_partitioned where dt is not null;
select * from partition_test_partitioned where dt is not null;
