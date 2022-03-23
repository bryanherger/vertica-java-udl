# vertica-java-udl
Custom UDSource, UDFilter, and UDParser extensions for Vertica developed in Java.  Currently adds support for reading JDBC sources as external tables (via parser), reading ActiveMQ queues via parser, and filtering and parsing XML and FIX types.
## Building
It's recommended that you install the Oracle JDK and configure Vertica to use that JVM.
Clone the repository as dbadmin on the target Vertica cluster.
The build.sh now uses Maven, but since Vertica JAR's and build info class are not available on Maven Central currently, you'll have to set this up manually.
Edit pom.xml to fix paths to Vertica JAR's and add dependencies for any JDBC drivers you need.
Edit build.sh and verify all settings, then run bash ./build.sh.  Currently this will build the JAR in /tmp, but you may want to put things elsewhere.
Testing:  Edit udparser.sql and verify all settings.  Note that udparser.sql expects JVM in the default location installed by the Oracle JDK RPM, and that all files were copied into /tmp by build.sh.  Also, it will create tables for the example data.
The example filters or parses small sample files and shows the resulting tables.
## Using the XML filter for flex tables
The XML filter takes two optional arguments: document, which is the tag used to split records in the XML (default "item"); and field_delimiter, which is used to concatenate multiple values for the same field (default ",").  See udparser.sql for a usage example.  You'll note that the XML parser extracts text content and attribute values.  Column names in Vertica table must match the nomenclature used by the parser; if you're not sure what the parser sees, check the Java UDxLogs, the parser will print a sample CREATE TABLE DDL to the log. The current limitations of the XML parser are that it loads all fields as VARCHAR currently, and that it has to load the entire file into memory on one node to build a DOM and process; this will limit input size and possibly have performance issues for very large files even in they fit in memory.
See udparser.sql and example.xml for a sample how to copy XML data into a flex table.
## Using the XML parser for regular tables
The XML parser takes two optional arguments: document, which is the tag used to split records in the XML (default "item"); and field_delimiter, which is used to concatenate multiple values for the same field (default ",").  See udparser.sql for a usage example.  You'll note that the XML parser extracts text content and attribute values.  Column names in Vertica table must match the nomenclature used by the parser; if you're not sure what the parser sees, check the Java UDxLogs, the parser will print a sample CREATE TABLE DDL to the log. The current limitations of the XML parser are that it loads all fields as VARCHAR currently, and that it has to load the entire file into memory on one node to build a DOM and process; this will limit input size and possibly have performance issues for very large files even in they fit in memory.
## Using the FIX filter for flex tables
The FIX parser currently takes no arguments and splits one or more FIX messages into fields and rows.  No field translation is done; field names will be "n" where n is the message code and the field value is copied as is.
## Using the FIX parser for regular tables
The FIX parser currently takes no arguments and splits one or more FIX messages into fields and rows.  No field translation is done; field names will be "FIXn" where n is the message code and the field value is copied as VARCHAR.
## Sourcing data via JDBC
The JDBCSource and Factory let you COPY data from a JDBC source.  It's still in progress but will eventually allow you to specify WHERE clause and partitions to read all data, just read data matching a WHERE clause, and divide the work across partitions so several or all nodes will be able to handle the workload.
The JDBCLoader lets you create any JDBC query as an external table - essentially, CREATE EXTERNAL TABLE jdbcTbl (cols...) AS COPY FROM JDBCLoader(query='') with the following caveat: the JDBC libraries must be packed into the JAR created by the build script, copy JDBC JAR files into lib folder (except vertica-jdbc.jar, which is already included!)  Also, column count and names from the JDBC query must line up with the external table definition.
## Sourcing data via Spark Sql
The SparkSql Source and Loader is modeled after the ODBC Loader and will eventually support similar features for predicate pushdown, column selection, and partitioning.
The SparkSqlLoader lets you create any Spark Sql query as an external table - essentially, CREATE EXTERNAL TABLE sparkTbl (cols...) AS COPY FROM SparkSqlLoader(query='') with the following caveat: the Vertica cluster (or subcluster!) executing the Spark Sql source and load must be able to access Spark, ideally by colocating Vertica and Spark installs.  More details will follow as I test this.
## Sourcing data via ActiveMQ
This is a UDSource implementation of the https://github.com/bryanherger/vertica-activemq project.  Unlike Kafka, this does not run on schedule and exits after reading current messages on the topic, so you'll need to run the COPY manually using a scheduling tool to load continuously. 
## Possible future enhancements
Proper type coercion attempt.
Using QuickFix/J to parse FIX.
Implement more X to JSON type parsers to chain for flex table input.

