package com.microfocus.vertica.mq;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Message;
import java.sql.PreparedStatement;
import java.util.Properties;

public class VerticaSink {

    private static final Logger LOG = LoggerFactory.getLogger(VerticaSink.class);

    private String INSERT_MESSAGE = "INSERT INTO shchema.table VALUES (?, ?, ?, ?) ";
    private ComboPooledDataSource cpds;
    private PreparedStatement statement;
    public long lastBatch = 0L;
    public Integer interval = new Integer(4000);
    public String dbUsername = "eventcep";
    public String dbPassword = "changeme";
    public String dbHostname = "localhost";
    public String dbPort = "5433";
    public String dbName = "streams";
    private Properties cfg;

    public VerticaSink() {
    }

    public VerticaSink(Properties c) {
        cfg = c;
        /***********************************************************************
         * Uncomment to use a Properties object to set parameters
         dbUsername = cfg.getProperty("dbuser");
         dbPassword = cfg.getProperty("dbpass");
         dbHostname = cfg.getProperty("dbhost");
         dbPort = cfg.getProperty("dbport");
         dbName = cfg.getProperty("dbname");
         INSERT_MESSAGE = cfg.getProperty("SQL");
         ************************************************************************/
    }

    // call invoke for
    public void invoke(Message anEvent) {
        try {
        /* Convert the Message to something we can INSERT, e.g.
        statement.setInt(1, anEvent.getIntProperty("id"));
        statement.setInt(2, anEvent.getIntProperty("val"));
        statement.setString(3, anEvent.getStringProperty("name"));
        */
            statement.addBatch();
            // this loop will commit records every X millis.  You could also count records and insert every X records.
            if ((System.currentTimeMillis() - lastBatch) > interval) {
                statement.executeBatch();
                lastBatch = System.currentTimeMillis();
            }
        } catch (Exception e) {
            LOG.error("Error in invoke()", e);
        }
    }

    public void open() {
        try {
            cpds = new ComboPooledDataSource();
            cpds.setDriverClass("com.vertica.jdbc.Driver"); //loads the jdbc driver
            cpds.setJdbcUrl("jdbc:vertica://" + dbHostname + ":" + dbPort + "/" + dbName);
            cpds.setUser(dbUsername);
            cpds.setPassword(dbPassword);
            cpds.setMaxStatements(180);
            statement = cpds.getConnection().prepareStatement(INSERT_MESSAGE);
            for (Object k : cfg.keySet()) {
                LOG.info("Properties key {} has value {}", k.toString(), cfg.get(k).toString());
            }
        } catch (Exception e) {
            LOG.error("Error in open()", e);
        }
    }

    public void close() {
        try {
            cpds.close();
        } catch (Exception e) {
            LOG.error("Error in close()", e);
        }
    }
}