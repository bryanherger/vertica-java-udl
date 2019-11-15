package com.bryanherger.udparser;

import com.vertica.sdk.*;

public class FixParserFactory extends ParserFactory {
    @Override
    public void getParameterType(ServerInterface srvInterface, SizedColumnTypes parameterTypes) {
        //parameterTypes.addVarchar(256, "document");
    }

    @Override
    public UDParser prepare(ServerInterface srvInterface, PerColumnParamReader perColumnParamReader,
                            PlanContext planCtxt, SizedColumnTypes returnType) {
        srvInterface.log("Class %s built with SDK version %s", this.getClass().getCanonicalName(), BuildInfo.get_vertica_build_info().sdk_version);
        return new FixParser(srvInterface);
    }
}
