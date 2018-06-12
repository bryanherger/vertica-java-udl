#!/bin/bash
cp /opt/vertica/sdk/BuildInfo.java src/com/vertica/sdk/BuildInfo.java
javac -classpath /opt/vertica/bin/VerticaSDK.jar src/com/vertica/sdk/BuildInfo.java src/com/bryanherger/udparser/*.java
cd src
jar cvf /tmp/xmludparser.jar com/vertica/sdk/BuildInfo.class com/bryanherger/udparser/*.class
cd ..
cp example.* /tmp
