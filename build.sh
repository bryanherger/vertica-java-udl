#!/bin/bash
mkdir -p src/main/java/com/vertica/sdk
cp /opt/vertica/sdk/BuildInfo.java src/main/java/com/vertica/sdk/BuildInfo.java
mvn clean package assembly:single
cp target/vertica-java-udl-0.1-jar-with-dependencies.jar /tmp

