CREATE OR REPLACE LIBRARY verticajavaudl AS '/tmp/verticajavaudl.jar' language 'java';
CREATE OR REPLACE PARSER JDBCLoader AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.JDBCLoaderFactory' LIBRARY verticajavaudl;
CREATE OR REPLACE SOURCE JDBCSource AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.JDBCSourceFactory' LIBRARY verticajavaudl;
COPY tbl WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='jdbc:postgresql://x:5432/x?user=x&password=x', query='select version();');
COPY tbl WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='jdbc:vertica://x:5433/x?user=x&password=x', query='select version();');
SELECT * FROM tbl;
