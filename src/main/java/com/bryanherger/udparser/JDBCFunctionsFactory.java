/*
Setup: ALTER DATABASE DEFAULT SET PARAMETER JavaBinaryForUDx = '/usr/bin/java';
Install:
CREATE LIBRARY javaUdllib AS '/tmp/vertica-java-udl-0.1-jar-with-dependencies.jar' LANGUAGE 'Java';
CREATE FUNCTION inferDdl AS language 'java' NAME 'com.bryanherger.udparser.JDBCFunctionsFactory' LIBRARY javaUdlLib;
 */

package com.bryanherger.udparser;

import com.vertica.sdk.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCFunctionsFactory extends ScalarFunctionFactory {
    @Override
    public void getPrototype(ServerInterface srvInterface,
                             ColumnTypes argTypes,
                             ColumnTypes returnType)
    {
        argTypes.addVarchar();
        argTypes.addVarchar();
        argTypes.addVarchar();
        returnType.addVarchar();
    }

    @Override
    public void getReturnType(ServerInterface srvInterface,
                              SizedColumnTypes argTypes,
                              SizedColumnTypes returnType) {
        argTypes.addVarchar(255);
        argTypes.addVarchar(255);
        argTypes.addVarchar(8);
        returnType.addVarchar(4096);
    }

    // This ScalarFunction is defined as an inner class of
    // its ScalarFunctionFactory class. This gets around having
    // to have a separate source file for this public class.
    public class JDBCFunctions extends ScalarFunction
    {
        public String getCreateTableDDL(String table, ResultSetMetaData rsmd) throws Exception {
            String sql = "CREATE TABLE IF NOT EXISTS "+table+" (";
            List<String> fields = new ArrayList<>();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                /* TODO: data type conversion */
                fields.add(rsmd.getColumnName(i)+" "+convertType(rsmd.getColumnTypeName(i), rsmd.getColumnDisplaySize(i)));
            }
            sql = sql + String.join(",", fields);
            sql = sql + ");";
            System.out.println(sql);
            return sql;
        }

        public String convertType(String inType, int inSize) {
            if ("serial".equalsIgnoreCase(inType)) { return "int"; }
            if ("text".equalsIgnoreCase(inType)) { return "varchar(32767)"; }
            if ("varchar".equalsIgnoreCase(inType)) { return "varchar("+inSize+")"; }
            return inType;
        }

        @Override
        public void processBlock(ServerInterface srvInterface,
                                 BlockReader argReader,
                                 BlockWriter resWriter)
                throws UdfException, DestroyInvocation
        {
            do {
                try {
                    String uri = argReader.getString(0);
                    String tbl = argReader.getString(1);
                    String dia = argReader.getString(2);
                    Connection cIn = DriverManager.getConnection(uri);
                    Statement sIn = cIn.createStatement();
                    /* TODO: LIMIT vs TOP here */
                    String sql = "SELECT * FROM " + tbl + " LIMIT 1;";
                    if ("TSQL".equalsIgnoreCase(dia)) {
                        sql = "SELECT TOP 1 * FROM " + tbl;
                    }
                    srvInterface.log("Getting metadata from source using: " + sql);
                    ResultSet rsIn = sIn.executeQuery(sql);
                    ResultSetMetaData rsmd = rsIn.getMetaData();
                    resWriter.setString(getCreateTableDDL(tbl, rsmd));
                } catch (Exception e) {
                    throw new UdfException(1, e.getMessage(), e);
                }
            } while (argReader.next());
        }
    }

    @Override
    public ScalarFunction createScalarFunction(ServerInterface srvInterface)
    {
        srvInterface.log(System.getProperty("java.class.path"));
        return new JDBCFunctions();
    }
}
