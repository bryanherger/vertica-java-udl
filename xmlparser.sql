ALTER DATABASE docker SET JavaBinaryForUDx = '/usr/java/latest/bin/java';

CREATE OR REPLACE LIBRARY verticajavaudl AS '/tmp/vertica-java-udl-0.1-jar-with-dependencies.jar' language 'java';

CREATE OR REPLACE PARSER XmlParser AS LANGUAGE 'java' NAME 'XmlParserFactory' LIBRARY verticajavaudl;

CREATE TABLE public.examplexml (tag1 VARCHAR, tag2 VARCHAR);

COPY public.examplexml FROM '/tmp/example.xml' PARSER XmlParser();
