CREATE OR REPLACE LIBRARY verticajavaudl AS '/data1/bryan/vertica-java-udl-0.1-jar-with-dependencies.jar' language 'java';
CREATE OR REPLACE PARSER ActiveMQLoader AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.ActiveMQLoaderFactory' LIBRARY verticajavaudl;
CREATE OR REPLACE SOURCE ActiveMQSource AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.ActiveMQSourceFactory' LIBRARY verticajavaudl;
COPY tbl WITH SOURCE ActiveMQSource() PARSER ActiveMQLoader(connect='tcp://localhost:61616', topic='verticaq');
SELECT * FROM tbl;
