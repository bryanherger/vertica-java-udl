package com.bryanherger.udparser;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.FileReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Jdbc2Vertica {
    /* TODO: make configurable */
    static String sourceUri = "";
    static String verticaUri = "";
    static String sourceTable = "";
    static String verticaTable = "";
    static String sourceUriKey = "srcuri";
    static String verticaUriKey = "dsturi";
    static String sourceTableKey = "srctbl";
    static String verticaTableKey = "dsttbl";
    static String sqlDialectKey = "dialect";
    static String sqlPartitionKey = "partition";
    static String sqlWhereKey = "where";

    public static String getCreateTableDDL(String table, ResultSetMetaData rsmd) throws Exception {
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

    public static String convertType(String inType, int inSize) {
        if ("serial".equalsIgnoreCase(inType)) { return "int"; }
        if ("text".equalsIgnoreCase(inType)) { return "varchar(32767)"; }
        if ("varchar".equalsIgnoreCase(inType)) { return "varchar("+inSize+")"; }
        return inType;
    }

    public static boolean isNotBlank(String str) {
        if (str == null) { return false; }
        if (str.trim().length() == 0) { return false; }
        return true;
    }

    public static void main(String[] argv) throws Exception {
        // parse command line args
        Args args = new Args();
        JCommander jc = JCommander.newBuilder()
                .addObject(args)
                .build();
        jc.parse(argv);

        if (args.help) {
            jc.usage();
            System.exit(0);
        }

        Properties params = new Properties();
        // load config file if specified and override
        if (args.propertiesFile != null) {
            params.load(new FileReader(args.propertiesFile));
        }

        if (isNotBlank(args.srcUri)) { params.setProperty(sourceUriKey, args.srcUri); }
        if (isNotBlank(args.srcTbl)) { params.setProperty(sourceTableKey, args.srcTbl); }
        if (isNotBlank(args.dstUri)) { params.setProperty(verticaUriKey, args.dstUri); }
        if (isNotBlank(args.dstTbl)) { params.setProperty(verticaTableKey, args.dstTbl); }
        if (isNotBlank(args.flavor)) { params.setProperty(sqlDialectKey, args.flavor); }
        if (isNotBlank(args.partition)) { params.setProperty(sqlPartitionKey, args.partition); }
        if (isNotBlank(args.where)) { params.setProperty(sqlWhereKey, args.where); }

        // connect to tables
        Connection cIn = DriverManager.getConnection(params.getProperty(sourceUriKey));
        Connection cVertica = DriverManager.getConnection(params.getProperty(verticaUriKey));

        Statement sIn = cIn.createStatement();
        /* TODO: LIMIT vs TOP here */
        String sql = "SELECT * FROM "+params.getProperty(sourceTableKey)+" LIMIT 1;";
        if ("TSQL".equalsIgnoreCase(params.getProperty(sqlDialectKey))) {
            sql = "SELECT TOP 1 * FROM "+params.getProperty(sourceTableKey);
        }
        System.out.println("Getting metadata from source using: "+sql);
        ResultSet rsIn = sIn.executeQuery(sql);

        Statement sVerticaDDL = cVertica.createStatement();
        ResultSetMetaData rsmd = rsIn.getMetaData();
        sVerticaDDL.execute(getCreateTableDDL(params.getProperty(verticaTableKey), rsmd));

        List<String> qs = new ArrayList<>();
        int cc = rsmd.getColumnCount();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            qs.add("?");
        }

        String ps = "INSERT INTO "+params.getProperty(verticaTableKey)+" VALUES ("+String.join(",",qs)+");";
        System.out.println(ps);
        PreparedStatement sVerticaPS = cVertica.prepareStatement(ps);

        /* TODO: handle partitions if configured, probably requires a loop/thread other than shown below */
        /* TODO: add ORDER and WHERE clauses here if configured */
        sql = "SELECT * FROM "+params.getProperty(sourceTableKey)+" "+params.getProperty(sqlWhereKey,"")+";";
        System.out.println(sql);
        rsIn = sIn.executeQuery(sql);
        int rc = 0;
        while (rsIn.next()) {
            for (int i = 1; i <= cc; i++) {
                sVerticaPS.setString(i, rsIn.getString(i));
            }
            rc = rc + 1;
            if (rc % 100 == 0) {
                // execute every 100 rows
                sVerticaPS.executeBatch();
            }
        }
        // flush
        sVerticaPS.executeBatch();
        System.out.println("Records copied = "+rc);
    }
}

class Args {
    // command line parsing: see http://jcommander.org/#_overview
    // in the help output from usage(), it looks like options are printed in order of long option name, regardless of order here
    @Parameter(names = {"-p","--properties"}, description = "Properties file (java.util.Properties format) (if omitted, use command line settings, or fall back to defaults)")
    public String propertiesFile = null;
    @Parameter(names = {"-h","-?","--help"}, help = true, description = "Print this message")
    public boolean help;
    @Parameter(names = {"--sourceUri"}, description = "Source JDBC URI")
    public String srcUri = "";
    @Parameter(names = {"--sourceTable"}, description = "Source Table (with schema, if needed)")
    public String srcTbl = "";
    @Parameter(names = {"--destUri"}, description = "Destination Vertica JDBC URI")
    public String dstUri = "";
    @Parameter(names = {"--destTable"}, description = "Destination Vertica Table (with schema, if needed)")
    public String dstTbl = "";
    @Parameter(names = {"--dialect"}, description = "SQL dialect (valid: 'PGSQL', 'TSQL')")
    public String flavor = "PGSQL";
    @Parameter(names = {"--partition"}, description = "SQL dialect (valid: 'PGSQL', 'TSQL')")
    public String partition = null;
    @Parameter(names = {"--where"}, description = "SQL dialect (valid: 'PGSQL', 'TSQL')")
    public String where = null;
}