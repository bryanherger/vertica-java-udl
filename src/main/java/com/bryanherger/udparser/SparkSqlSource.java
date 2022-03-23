package com.bryanherger.udparser;

import com.vertica.sdk.DataBuffer;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.State.StreamState;
import com.vertica.sdk.UDSource;
import com.vertica.sdk.UdfException;

import static com.bryanherger.udparser.JDBCSource.logMemoryUsage;

public class SparkSqlSource extends UDSource {

    public SparkSqlSource() {
        super();
    }

    @Override
    public StreamState process(ServerInterface srvInterface, DataBuffer output) throws UdfException {
        logMemoryUsage(srvInterface);
        if (output.buf.length < 1) return StreamState.OUTPUT_NEEDED;
        output.offset = 1;
        return StreamState.DONE;
    }

}
