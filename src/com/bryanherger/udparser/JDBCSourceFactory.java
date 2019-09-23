package com.bryanherger.udparser;

import com.vertica.sdk.*;
import java.util.ArrayList;

public class JDBCSourceFactory extends SourceFactory {
    @Override
    public void plan(ServerInterface srvInterface,
                      NodeSpecifyingPlanContext planCtxt) {
        // Make the query only run on the current node.
        ArrayList<String> nodes = new ArrayList<String>();
        nodes.add(srvInterface.getCurrentNodeName());
        planCtxt.setTargetNodes(nodes);
    }

    @Override
    public ArrayList<UDSource> prepareUDSources(ServerInterface srvInterface,
                                                    NodeSpecifyingPlanContext planCtxt) {
        ArrayList<UDSource> retVal = new ArrayList<UDSource>();
        retVal.add(new JDBCSource());
        return retVal;
    }
}
