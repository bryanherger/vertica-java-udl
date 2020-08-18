package com.bryanherger.udparser;

import com.vertica.sdk.DataBuffer;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.State.StreamState;
import com.vertica.sdk.UDSource;

public class JDBCFunctions extends UDSource {
    @Override
    public StreamState process(ServerInterface srvInterface, DataBuffer output) {
        if (output.buf.length < 1) return StreamState.OUTPUT_NEEDED;
        output.offset = 1;
        return StreamState.DONE;
    }
}
