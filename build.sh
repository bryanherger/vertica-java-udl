#!/bin/bash
cp /opt/vertica/java/vertica-jdbc.jar lib/
mkdir -p src/main/java/com/vertica/sdk
cp /opt/vertica/sdk/BuildInfo.java src/main/java/com/vertica/sdk/BuildInfo.java
javac -classpath /opt/vertica/bin/VerticaSDK.jar src/main/java/com/vertica/sdk/BuildInfo.java src/main/java/com/bryanherger/udparser/*.java
rm -rf build
mkdir -p build
cd build
mkdir -p com/vertica/sdk
mkdir -p com/bryanherger/udparser
cp ../src/main/java/com/vertica/sdk/*.class com/vertica/sdk
cp ../src/main/java/com/bryanherger/udparser/*.class com/bryanherger/udparser
find ../lib -name "*.jar" | xargs -n 1 jar xf
jar cvf /tmp/verticajavaudl.jar *
cd ..
cp example.* /tmp
