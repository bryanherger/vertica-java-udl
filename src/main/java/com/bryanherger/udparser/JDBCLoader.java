package com.bryanherger.udparser;

import com.vertica.sdk.*;
import com.vertica.sdk.State.InputState;
import com.vertica.sdk.State.StreamState;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JDBCLoader extends UDParser {
    Connection c;
    Statement s;
    ResultSet rs;

    @Override
    public StreamState process(ServerInterface srvInterface, DataBuffer input, InputState inputState) {
        Map<String, Object> map = new HashMap<>();
        StreamWriter output = getStreamWriter();
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            srvInterface.log(""+rsmd.getColumnCount());
            while (rs.next()) {
                map.clear();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    try {
                        srvInterface.log(""+i+"|"+rsmd.getColumnName(i)+"|"+rs.getString(i));
                        map.put(rsmd.getColumnName(i), rs.getString(i));
                    } catch (Exception se) {

                    }
                }
                output.setRowFromMap(map);
                output.next();
            }
        } catch (Exception se) {
            throw new UdfException(0, "process Exception: "+se.getMessage());
        } catch (DestroyInvocation de) {
            throw new UdfException(0, "process DI Exception: "+de.getMessage());
        }
        return StreamState.DONE;
    }

    @Override
    public void setup(ServerInterface srvInterface, SizedColumnTypes returnType) {
        // Connection string, passed in as an argument
        String connect = srvInterface.getParamReader().getString("connect");
        String query = srvInterface.getParamReader().getString("query");

        // Establish JDBC connection
        try {
            c = DriverManager.getConnection(connect);
        } catch (Exception se) {
            throw new UdfException(0, "Error getting JDBC Connection: "+se.getMessage());
        }

        // run the Statement
        try {
            s = c.createStatement();
            rs = s.executeQuery(query);
        } catch (Exception se) {
            throw new UdfException(0, "Error running JDBC query: "+se.getMessage());
        }
    }

    @Override
    public void destroy(ServerInterface srvInterface, SizedColumnTypes returnType) {
        // Try to free even on error, to minimize the risk of memory leaks.
        // But do check for errors in the end.
        try {
            rs.close();
        } catch (Exception se) {
        }
        try {
            s.close();
        } catch (Exception se) {
        }
        try {
            c.close();
        } catch (Exception se) {
        }
    }
}
