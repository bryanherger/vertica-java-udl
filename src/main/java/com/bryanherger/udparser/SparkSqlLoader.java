package com.bryanherger.udparser;

import com.vertica.sdk.*;
import com.vertica.sdk.State.InputState;
import com.vertica.sdk.State.StreamState;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.bryanherger.udparser.JDBCSource.logMemoryUsage;
import static com.vertica.sdk.State.StreamState.DONE;
import static com.vertica.sdk.State.StreamState.OUTPUT_NEEDED;

public class SparkSqlLoader extends UDParser {
    private String sqlQuery;
    @Override
    public StreamState process(ServerInterface srvInterface, DataBuffer input, InputState inputState) {
        logMemoryUsage(srvInterface);
        // columns are 0-index here, unlike JDBC...
        writer.setLong(0, 1L);
        writer.setString(1, "foo");
        try {
            boolean next = writer.next();
            srvInterface.log("writer.next: %s", Boolean.toString(next));
        } catch (DestroyInvocation destroyInvocation) {
            srvInterface.log("Exception: %s", destroyInvocation.getMessage());
        }
        return DONE;
    }

    @Override
    public void setup(ServerInterface srvInterface, SizedColumnTypes returnType) {
        //SparkSqlLoader - key|value __query_col_idx__|0
        //SparkSqlLoader - key|value __pred_0__|(id < 100)
        //SparkSqlLoader - key|value __query_col_name__|id
        //SparkSqlLoader - key|value query|select * from iceberg;
        logMemoryUsage(srvInterface);
        String query = "", qcol = "";
        List<String> qpred = new ArrayList<String>();
        for (String key : srvInterface.getParamReader().getParamNames()) {
            String value = srvInterface.getParamReader().getString(key);
            srvInterface.log("key|value %s|%s", key, value);
            if ("query".equals(key)) { query = value; }
            if ("__query_col_name__".equals(key)) { qcol = value; }
            if (key.startsWith("__pred_")) { qpred.add(value); }
        }
        query = query.replace(";","");
        query = query.replace("*",qcol);
        if (qpred.size() > 0) {
            query = query + " WHERE " + String.join(" AND ", qpred);
        }
        sqlQuery = query;
        srvInterface.log("sqlQuery %s", query);
        logMemoryUsage(srvInterface);
    }

    @Override
    public void destroy(ServerInterface srvInterface, SizedColumnTypes returnType) {
    }
}
