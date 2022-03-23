package com.bryanherger.udparser;

import com.vertica.sdk.DataBuffer;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.State.StreamState;
import com.vertica.sdk.UDSource;
import com.vertica.sdk.UdfException;

import java.io.ByteArrayInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JDBCSource extends UDSource {

    public JDBCSource(String jdbcUri, String tablename, String query) {
        super();
        this.jdbcUri = jdbcUri;
        this.tableName = tablename;
        this.query = query;
    }

    public static void logMemoryUsage(ServerInterface srvInterface) {
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        srvInterface.log("Memory: Used %d, Free %d, Total %d", total - free, free, total);
    }

    @Override
    public void setup(ServerInterface srvInterface) throws UdfException {
        try {
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", pass);
            c = DriverManager.getConnection(jdbcUri, props);
            s = c.createStatement();
            rs = s.executeQuery(query);
            rsmd = rs.getMetaData();
        } catch (Exception e) {
            throw new UdfException(0, e.getMessage(), e);
        }
    }

    @Override
    public void destroy(ServerInterface srvInterface) throws UdfException {
        if (rs != null)
            try {
                rs.close();
                s.close();
                c.close();
            } catch (Exception e) {
                String msg = e.getMessage();
                throw new UdfException(0, msg);
            }
    }

    @Override
    public StreamState process(ServerInterface srvInterface, DataBuffer output) throws UdfException {
        //make sure we don't overwrite the already existing stuff
        long offset;
        try {
            logMemoryUsage(srvInterface);
            while (rs.next()) {
                List<String> thisRow = new ArrayList<>();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    try {
                        //srvInterface.log("" + i + "|" + rsmd.getColumnName(i) + "|" + rs.getString(i));
                        String fld = rs.getString(i);
                        if (fld != null) {
                            fld = fld.replace('\n',' ').replace("|","\\|");
                        }
                        thisRow.add(fld);
                    } catch (Exception se) {
                        srvInterface.log("Exception: " + se.getMessage() + " on:" + i + "|" + rsmd.getColumnName(i) + "|" + rs.getString(i));
                    }
                }
                //StringReader sr = new StringReader(String.join("|", thisRow));
                ByteArrayInputStream bais = new ByteArrayInputStream((String.join("|", thisRow) + "\n").getBytes("UTF-8"));
                offset = bais.read(output.buf, output.offset, output.buf.length - output.offset);
                output.offset += offset;
                //srvInterface.log("current offset = " + output.offset);
                if (output.offset > 900000) {
                    return StreamState.OUTPUT_NEEDED;
                }
            }
            return StreamState.DONE;
            /*if (rs.next()) {
                logMemoryUsage(srvInterface);
                List<String> thisRow = new ArrayList<>();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    try {
                        srvInterface.log(""+i+"|"+rsmd.getColumnName(i)+"|"+rs.getString(i));
                        thisRow.add(rs.getString(i));
                    } catch (Exception se) {
                        srvInterface.log("Exception: "+se.getMessage()+" on:"+i+"|"+rsmd.getColumnName(i)+"|"+rs.getString(i));
                    }
                }
                //StringReader sr = new StringReader(String.join("|", thisRow));
                ByteArrayInputStream bais = new ByteArrayInputStream(String.join("|", thisRow).getBytes("UTF-8"));
                offset = bais.read(output.buf,output.offset,output.buf.length-output.offset);
                srvInterface.log("current offset = "+offset);
                return StreamState.OUTPUT_NEEDED;
            } else {
                // no more rows
                return StreamState.DONE;
            }*/
        } catch (Exception e) {
            String msg = e.getMessage();
            throw new UdfException(0, msg);
        }

        /*output.offset +=offset;
        if (offset == -1 || offset < output.buf.length) {
        return StreamState.DONE;
        } else {
            return StreamState.OUTPUT_NEEDED;
        }*/

    }

    private String tableName, jdbcUri = "", user = "sa", pass = "sa", query;
    Connection c;
    Statement s;
    ResultSet rs;
    ResultSetMetaData rsmd;
}
