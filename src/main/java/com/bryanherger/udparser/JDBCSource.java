package com.bryanherger.udparser;

import com.vertica.sdk.*;
import com.vertica.sdk.State.InputState;
import com.vertica.sdk.State.StreamState;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;

public class JDBCSource extends UDSource {

    public JDBCSource(String jdbcUri, String tablename, String query) {
        super();
        this.jdbcUri = jdbcUri;
        this.tableName = tablename;
        this.query = query;
    }

    public void logMemoryUsage(ServerInterface srvInterface) {
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
            while (rs.next()) {
                logMemoryUsage(srvInterface);
                List<String> thisRow = new ArrayList<>();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    try {
                        srvInterface.log("" + i + "|" + rsmd.getColumnName(i) + "|" + rs.getString(i));
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
                srvInterface.log("current offset = " + output.offset);
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
