-- create library
DROP LIBRARY verticajavaudl CASCADE;
--CREATE OR REPLACE LIBRARY verticajavaudl AS '/tmp/vertica-java-udl-0.1-jar-with-dependencies.jar' language 'java';
-- copy JAR's from /usr/lib/spark/jars/*.jar to /tmp/spark-jars (the *.jar part is important to avoid copying crap from aws install)
CREATE OR REPLACE LIBRARY verticajavaudl AS '/tmp/vertica-java-udl-0.1.jar' depends '/tmp/spark-jars' language 'java';
CREATE OR REPLACE PARSER SparkSqlLoader AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.spark.SparkSqlLoaderFactory' LIBRARY verticajavaudl;
CREATE OR REPLACE SOURCE SparkSqlSource AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.spark.SparkSqlSourceFactory' LIBRARY verticajavaudl;
-- test
CREATE EXTERNAL TABLE sparksqltbl (id int, datum varchar) AS COPY WITH SOURCE SparkSqlSource() PARSER SparkSqlLoader(query='select * from tbl1');
SELECT * FROM sparksqltbl;
SELECT * FROM sparksqltbl where id < 200;
SELECT id FROM sparksqltbl where id < 200;
DROP TABLE sparksqltbl;
