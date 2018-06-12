ALTER DATABASE docker SET JavaBinaryForUDx = '/usr/java/latest/bin/java';

CREATE OR REPLACE LIBRARY xmludparser AS '/tmp/xmludparser.jar' language 'java';

CREATE OR REPLACE PARSER XmlParser AS LANGUAGE 'java' NAME 'XmlParserFactory' LIBRARY xmludparser;

CREATE TABLE public.examplexml (tag1 VARCHAR, tag2 VARCHAR);

COPY public.examplexml FROM '/tmp/example.xml' PARSER XmlParser();
