#!/bin/bash
 cp /opt/vertica/sdk/BuildInfo.java com/vertica/sdk/BuildInfo.java
javac -classpath /opt/vertica/bin/VerticaSDK.jar com/vertica/sdk/BuildInfo.java com/bryanherger/udparser/*.java
jar cvf /tmp/xmludparser.jar com/vertica/sdk/BuildInfo.class com/bryanherger/udparser/*.class
