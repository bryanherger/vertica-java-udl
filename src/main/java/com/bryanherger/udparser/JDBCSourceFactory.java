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
import java.util.Collections;

public class JDBCSourceFactory extends SourceFactory {
    @Override
    public void plan(ServerInterface srvInterface,
                     NodeSpecifyingPlanContext planCtxt) throws UdfException {
        if (!srvInterface.getParamReader().containsParameter("tbl")) {
            throw new UdfException(0, "Required parameter \"tbl\" not found");
        }
        if (srvInterface.getParamReader().containsParameter("jdbcUri")) {
            jdbcUri = srvInterface.getParamReader().getString("jdbcUri");
        }
        if (srvInterface.getParamReader().containsParameter("conditions")) {
            where = srvInterface.getParamReader().getString("conditions");
        }
        if (srvInterface.getParamReader().containsParameter("partitions")) {
            partition = srvInterface.getParamReader().getString("partitions");
        }
        String classpath = System.getProperty("java.class.path");
        srvInterface.log("Classpath:"+classpath);
        findExecutionNodes(srvInterface.getParamReader(), planCtxt, srvInterface.getCurrentNodeName());
    }

    @Override
    public void getParameterType(ServerInterface srvInterface, SizedColumnTypes parameterTypes) {
        parameterTypes.addVarchar(256, "tbl");
        parameterTypes.addVarchar(256, "conditions");
        parameterTypes.addVarchar(256, "partitions");
        parameterTypes.addVarchar(65000, "jdbcUri");
        parameterTypes.addVarchar(65000, "nodes");
    }

    @Override
    public ArrayList<UDSource> prepareUDSources(ServerInterface srvInterface,
                                                NodeSpecifyingPlanContext planCtxt) throws UdfException {
        //TODO
        // Do glob expansion; if the path contains '*', find all matching files.
        // Note that this has to be done in the prepare() method:
        // plan() runs on the initiator node, which may be a totally different
        // computer from the execution node that runs the actual query.
        // If we're trying to load a particular, say, directory full of files
        // on the local filesystem of the execution node, well, plan() doesn't
        // see the execution node's local filesystem, it sees the initiator's
        // local filesystem, so a glob expansion won't work at all.
        // prepare(), on the other hand, runs on the execution node.  So it's
        // fine to access local files and resources.

        table = srvInterface.getParamReader().getString("tbl");
        ArrayList<UDSource> sources = new ArrayList<UDSource>();
        int nodes = planCtxt.getTargetNodes().size();
        int splits = nodes; // TODO: splits = partitions, not nodes
        srvInterface.log("Nodes:"+nodes+",Splits:"+splits);
        String whereClause = "";
        for (int i = 0; i < splits; i++) {
            sources.add(new JDBCSource(jdbcUri, table+"_"+i, "SELECT * FROM "+table+" WHERE id % "+splits+" = "+i));
    }
        return sources;

        /*String filename = srvInterface.getParamReader().getString("file");

        if (srvInterface.getParamReader().containsParameter("file_split_regex")) {
            ArrayList<UDSource> sources = new ArrayList<UDSource>();
            String[] fileNames = filename.split(srvInterface.getParamReader().getString("file_split_regex"));
            for (int i = 0; i < fileNames.length; i++) {
                sources.add(new JdbcSource(fileNames[i]));
            }
            return sources;
        } else {
            return new ArrayList<UDSource>(Collections.singletonList(new JdbcSource(filename)));
        }*/
    }

    private void findExecutionNodes(ParamReader args,
                                    NodeSpecifyingPlanContext planCtxt, String defaultList) throws UdfException {
        String nodes;
        ArrayList<String> clusterNodes = new ArrayList<String>(planCtxt.getClusterNodes());
        ArrayList<String> executionNodes = new ArrayList<String>();

        // If we found the nodes arg,
        if (args.containsParameter("nodes")) {
            nodes = args.getString("nodes");
        } else if (defaultList != "" ) {
            nodes = defaultList;
        } else {
            // use all nodes by default?
            nodes = "ALL NODES";
            // We have nothing to do here.
            //return;
        }

        // Check for special magic values first
        if (nodes == "ALL NODES") {
            executionNodes = clusterNodes;
        } else if (nodes == "ANY NODE") {
            Collections.shuffle(clusterNodes);
            executionNodes.add(clusterNodes.get(0));
        } else if (nodes == "") {
            // Return the empty nodes list.
            // Vertica will deal with this case properly.
        } else {
            // Have to actually parse the string
            // "nodes" is a comma-separated list of node names.
            String[] nodeNames = nodes.split(",");

            for (int i = 0; i < nodeNames.length; i++){
                if (clusterNodes.contains(nodeNames[i])) {
                    executionNodes.add(nodeNames[i]);
                } else {
                    String msg = String.format("Specified node '%s' but no node by that name is available.  Available nodes are \"%s\".",
                            nodeNames[i], clusterNodes.toString());
                    throw new UdfException(0, msg);
                }
            }
        }

        planCtxt.setTargetNodes(executionNodes);
    }

    private String jdbcUri = "jdbc:postgresql://192.168.1.206:5432/commafeed?user=sa&password=sa";
    private String table, tbl, where = "", partition = "id";
}
