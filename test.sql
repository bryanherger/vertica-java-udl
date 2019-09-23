CREATE OR REPLACE LIBRARY xmludparser AS '/tmp/xmludparser.jar' language 'java';
CREATE OR REPLACE PARSER XmlParser AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.XmlParserFactory' LIBRARY xmludparser;
CREATE OR REPLACE PARSER FixParser AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.FixParserFactory' LIBRARY xmludparser;
CREATE OR REPLACE FILTER XmlFilter AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.XmlFilterFactory' LIBRARY xmludparser;
CREATE OR REPLACE PARSER JDBCLoader AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.JDBCLoaderFactory' LIBRARY xmludparser;
CREATE OR REPLACE SOURCE JDBCSource AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.JDBCSourceFactory' LIBRARY xmludparser;
COPY tbl WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='jdbc:postgresql://192.168.1.206:5432/commafeed?user=sa&password=sa', query='select version();');

