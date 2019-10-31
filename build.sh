#!/bin/bash
cp /opt/vertica/java/vertica-jdbc.jar lib/
mkdir -p src/com/vertica/sdk
cp /opt/vertica/sdk/BuildInfo.java src/com/vertica/sdk/BuildInfo.java
javac -classpath /opt/vertica/bin/VerticaSDK.jar src/com/vertica/sdk/BuildInfo.java src/com/bryanherger/udparser/*.java
rm -rf build
mkdir -p build
cd build
mkdir -p com/vertica/sdk
mkdir -p com/bryanherger/udparser
cp ../src/com/vertica/sdk/*.class com/vertica/sdk
cp ../src/com/bryanherger/udparser/*.class com/bryanherger/udparser
find ../lib -name "*.jar" | xargs -n 1 jar xf
jar cvf /tmp/xmludparser.jar *
cd ..
cp example.* /tmp
