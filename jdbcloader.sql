CREATE OR REPLACE LIBRARY xmludparser AS '/tmp/xmludparser.jar' language 'java';
CREATE OR REPLACE PARSER JDBCLoader AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.JDBCLoaderFactory' LIBRARY xmludparser;
CREATE OR REPLACE SOURCE JDBCSource AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.JDBCSourceFactory' LIBRARY xmludparser;
COPY tbl WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='jdbc:postgresql://x:5432/x?user=x&password=x', query='select version();');
COPY tbl WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='jdbc:vertica://x:5433/x?user=x&password=x', query='select version();');
SELECT * FROM tbl;
