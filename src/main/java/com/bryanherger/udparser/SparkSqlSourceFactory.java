/*
Setup: ALTER DATABASE DEFAULT SET PARAMETER JavaBinaryForUDx = '/usr/bin/java';
Install:
CREATE LIBRARY javaUdllib AS '/tmp/vertica-java-udl-0.1-jar-with-dependencies.jar' LANGUAGE 'Java';
CREATE FUNCTION inferDdl AS language 'java' NAME 'com.bryanherger.udparser.JDBCFunctionsFactory' LIBRARY javaUdlLib;
CREATE SOURCE jdbcSource as LANGUAGE 'JAVA' NAME 'com.bryanherger.udparser.JDBCSourceFactory' LIBRARY javaUdlLib;
Teardown:
DROP LIBRARY javaUdlLib CASCADE;
drop table jdbc_rss_feeds ;
 */

package com.bryanherger.udparser;

import com.vertica.sdk.*;

import java.util.ArrayList;

public class SparkSqlSourceFactory extends SourceFactory {
    @Override
    public void plan(ServerInterface srvInterface,
                     NodeSpecifyingPlanContext planCtxt) throws UdfException {
        ArrayList<String> nodes = new ArrayList<String>();
        nodes.add(srvInterface.getCurrentNodeName());
        planCtxt.setTargetNodes(nodes);
    }

    @Override
    public ArrayList<UDSource> prepareUDSources(ServerInterface srvInterface,
                                                NodeSpecifyingPlanContext planCtxt) throws UdfException {
        ArrayList<UDSource> sources = new ArrayList<UDSource>();
        sources.add(new SparkSqlSource());
        return sources;
    }
}
