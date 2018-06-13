# vertica-custom-udparsers
Custom UDParser extensions for Vertica developed in Java.  Currently adds support for XML types.  FIX coming soon.
## Building
It's recommended that you install the Oracle JDK and configure Vertica to use that JVM.
Clone the repository as dbadmin.
Edit build.sh and verify all settings.  Currently this will build the JAR and copy examples in /tmp, but you may want to put things elsewhere.
Edit udparser.sql and verify all settings.  Note that udparser.sql expects JVM in the default location installed by the Oracle JDK RPM, and that all files were copied into /tmp by build.sh.  Also, it will create tables for the example data.
The example parses small sample files and shows the resulting tables.
## Using the XML parser
The XML parser takes two optional arguments: document, which is the tag used to split records in the XML (default "item"); and field_delimiter, which is used to concatenate multiple values for the same field (default ",").  See udparser.sql for a usage example.  You'll note that the XML parser extracts text content and attribute values.  Column names in Vertica table must match the nomenclature used by the parser; if you're not sure what the parser sees, check the Java UDxLogs, the parser will print a sample CREATE TABLE DDL to the log. The current limitations of the XML parser are that it loads all fields as VARCHAR currently, and that it has to load the entire file into memory on one node to build a DOM and process; this will limit input size and possibly have performance issues for very large files even in they fit in memory.

