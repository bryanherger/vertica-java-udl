package com.bryanherger.udparser;

import com.vertica.sdk.*;

public class ActiveMQFilterFactory extends ParserFactory {
    @Override
    public void plan(ServerInterface srvInterface,
                     PerColumnParamReader perColumnParamReader,
                     PlanContext planCtxt) throws UdfException {
        if (!srvInterface.getParamReader().containsParameter("connect")) {
            throw new UdfException(0, "Error:  ActiveMQLoader requires a 'connect' string containing ActiveMQ broker URL");
        }
        if (!srvInterface.getParamReader().containsParameter("topic")) {
            throw new UdfException(0, "Error:  ActiveMQLoader requires a 'topic' string, the topic to ingest");
        }
    }

    @Override
    public UDParser prepare(ServerInterface srvInterface,
                            PerColumnParamReader perColumnParamReader,
                            PlanContext planCtxt,
                            SizedColumnTypes returnType) {
        return new ActiveMQFilter();
    }

    @Override
    public void getParameterType(ServerInterface srvInterface,
                                 SizedColumnTypes parameterTypes) {
        parameterTypes.addVarchar(255, "connect");
        parameterTypes.addVarchar(255, "topic");
        parameterTypes.addInt("messages");
    }
}
