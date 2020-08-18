#!/bin/bash
java -cp /opt/vertica/java/vertica-jdbc.jar:/usr/share/java/postgresql-jdbc.jar:vertica-java-udl-0.1-jar-with-dependencies.jar com.bryanherger.udparser.Jdbc2Vertica --sourceUri jdbc:postgresql://192.168.1.206:5432/commafeed?user=sa\&password=sa --sourceTable rss.rss_feeds --destUri jdbc:vertica://192.168.1.206:5433/d2?user=dbadmin\&password=Vertica1\! --destTable rss_feeds

