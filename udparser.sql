ALTER DATABASE DEFAULT SET JavaBinaryForUDx = '/usr/java/latest/bin/java';

CREATE OR REPLACE LIBRARY xmludparser AS '/tmp/xmludparser.jar' language 'java';

CREATE OR REPLACE PARSER XmlParser AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.XmlParserFactory' LIBRARY xmludparser;
CREATE OR REPLACE PARSER FixParser AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.FixParserFactory' LIBRARY xmludparser;
CREATE OR REPLACE FILTER XmlFilter AS LANGUAGE 'java' NAME 'com.bryanherger.udparser.XmlFilterFactory' LIBRARY xmludparser;

DROP TABLE IF EXISTS public.examplexml;
CREATE TABLE public.examplexml (author_text VARCHAR, title_text VARCHAR, title_attr_lang VARCHAR, year_text VARCHAR, price_text VARCHAR);
COPY public.examplexml FROM LOCAL 'example.xml' PARSER XmlParser(document='book',field_delimiter=';');
SELECT * FROM public.examplexml;

DROP TABLE IF EXISTS public.examplexmlflex;
CREATE FLEX TABLE public.examplexmlflex();
COPY public.examplexmlflex FROM LOCAL 'example.xml' FILTER XmlFilter(document='book',field_delimiter=';') PARSER FJSONPARSER();
SELECT * FROM public.examplexmlflex;

DROP TABLE IF EXISTS public.examplefix;
CREATE TABLE public.examplefix (FIX44 VARCHAR, FIX45 VARCHAR, FIX49 VARCHAR, FIX150 VARCHAR, FIX151 VARCHAR, FIX52 VARCHAR, FIX31 VARCHAR, FIX98 VARCHAR, FIX10 VARCHAR, FIX54 VARCHAR, FIX32 VARCHAR, FIX11 VARCHAR, FIX55 VARCHAR, FIX34 VARCHAR, FIX56 VARCHAR, FIX35 VARCHAR, FIX14 VARCHAR, FIX58 VARCHAR, FIX59 VARCHAR, FIX37 VARCHAR, FIX38 VARCHAR, FIX17 VARCHAR, FIX39 VARCHAR, FIX6 VARCHAR, FIX8 VARCHAR, FIX9 VARCHAR, FIX108 VARCHAR, FIX40 VARCHAR, FIX41 VARCHAR, FIX20 VARCHAR, FIX21 VARCHAR);
COPY public.examplefix FROM LOCAL 'example.fix' PARSER FixParser();
SELECT * FROM public.examplefix;
