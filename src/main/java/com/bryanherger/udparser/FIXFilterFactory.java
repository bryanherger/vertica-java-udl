package com.bryanherger.udparser;

import com.vertica.sdk.*;

public class FIXFilterFactory extends FilterFactory {
    @Override
    public UDFilter prepare(ServerInterface srvInterface, PlanContext planCtxt) throws UdfException {
        srvInterface.log("Class %s built with SDK version %s", this.getClass().getCanonicalName(), BuildInfo.get_vertica_build_info().sdk_version);
        return new FIXFilter(srvInterface);
    }

    @Override
    public void getParameterType(ServerInterface srvInterface,
            SizedColumnTypes parameterTypes) {
    }
}
