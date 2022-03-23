-- create library
DROP LIBRARY verticajavaudl CASCADE;
CREATE OR REPLACE LIBRARY verticajavaudl AS '/tmp/vertica-java-udl-0.1-jar-with-dependencies.jar' language 'java';
CREATE OR REPLACE PARSER SparkSqlLoader AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.SparkSqlLoaderFactory' LIBRARY verticajavaudl;
CREATE OR REPLACE SOURCE SparkSqlSource AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.SparkSqlSourceFactory' LIBRARY verticajavaudl;
-- test
CREATE EXTERNAL TABLE sparksqltbl (id int, datum varchar) AS COPY WITH SOURCE SparkSqlSource() PARSER SparkSqlLoader(connect='jdbc:vertica://x:5433/x?user=x&password=x', query='select * from iceberg;');
SELECT * FROM sparksqltbl;
SELECT * FROM sparksqltbl where id < 100;
SELECT id FROM sparksqltbl where id < 100;
DROP TABLE sparksqltbl;
