package com.bryanherger.udparser.spark;

import com.vertica.sdk.*;
import com.vertica.sdk.State.InputState;
import com.vertica.sdk.State.StreamState;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static com.bryanherger.udparser.JDBCSource.logMemoryUsage;
import static com.vertica.sdk.State.StreamState.DONE;

public class SparkSqlLoader extends UDParser {
    private String sqlQuery,
            sparkMaster = "local[1]",
            spark_sql_warehouse_dir = "hdfs:///user/spark/warehouse",
            spark_hive_metastore_uris = "thrift://ip-172-31-26-110.ec2.internal:9083";
    private static ServerInterface si;
    private static com.vertica.sdk.StreamWriter sw;
    private static ForeachFunction<Row> fef = new ForeachFunction<Row>() {
        @Override
        public void call(Row row) throws Exception {
            si.log("Row = %s", row.toString());
            for (int i = 0; i < row.size(); i++) {
                Object o = row.get(i);
                if (o instanceof Integer) {
                    sw.setLong(i, (Integer) o);
                }
                if (o instanceof Long) {
                    sw.setLong(i, (Long) o);
                }
                if (o instanceof String) {
                    sw.setString(i, (String) o);
                }
                //sw.setLong(0, 123L);
                //sw.setString(1, "FourFiveSix");
            }
            try {
                boolean next = sw.next();
            } catch (DestroyInvocation destroyInvocation) {
            }
        }
    };

    @Override
    public StreamState process(ServerInterface srvInterface, DataBuffer input, InputState inputState) {
        logMemoryUsage(srvInterface);
        /*SparkSession spark = SparkSession
      .builder()
      .appName("Vertica UDParser SparkSqlLoader")
      .master(sparkMaster)
      .config("spark.sql.warehouse.dir",spark_sql_warehouse_dir)
      .config("hive.metastore.uris",spark_hive_metastore_uris)
      .enableHiveSupport()
      .getOrCreate();*/
        // get a canned sample for AWS EMR 6.5 test
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master(sparkMaster)
                .config("spark.sql.warehouse.dir", spark_sql_warehouse_dir)
                .config("hive.metastore.uris", spark_hive_metastore_uris)
                .config("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName())
                .config("*.sink.console.class", org.apache.spark.metrics.sink.ConsoleSink.class.getName())
                .enableHiveSupport()
                .getOrCreate();
        Dataset<Row> resultSet = spark.sql(sqlQuery/*"SELECT * FROM tbl1"*/);
        String rows = resultSet.toString();
        srvInterface.log("rows = %s", rows);
        /*resultSet.foreach( row -> {
            try {
                srvInterface.log("row.size: %d", row.size());
                for (int i = 0;  i < row.size(); i++) {
                    writer.setString(i, row.getString(i));
                }
                boolean next = writer.next();
            } catch (DestroyInvocation e) {
                srvInterface.log("Exception when writing row: %s", e.getMessage());
            }
        });*/
        si = srvInterface;
        sw = writer;
        resultSet.foreach(fef);
        // columns are 0-index in writer, unlike JDBC...
        return DONE;
    }

    @Override
    public void setup(ServerInterface srvInterface, SizedColumnTypes returnType) {
        //SparkSqlLoader - key|value __query_col_idx__|0
        //SparkSqlLoader - key|value __pred_0__|(id < 100)
        //SparkSqlLoader - key|value __query_col_name__|id
        //SparkSqlLoader - key|value query|select * from iceberg;
        //        parameterTypes.addVarchar(65000, "spark_sql_warehouse_dir");
        //        parameterTypes.addVarchar(65000, "spark_hive_metastore_uris");
        logMemoryUsage(srvInterface);
        String query = "", qcol = "";
        List<String> qpred = new ArrayList<String>();
        for (String key : srvInterface.getParamReader().getParamNames()) {
            String value = srvInterface.getParamReader().getString(key);
            srvInterface.log("key|value %s|%s", key, value);
            if ("query".equals(key)) {
                query = value;
            }
            if ("spark_master".equals(key)) {
                sparkMaster = value;
            }
            if ("spark_sql_warehouse_dir".equals(key)) {
                spark_sql_warehouse_dir = value;
            }
            if ("spark_hive_metastore_uris".equals(key)) {
                spark_hive_metastore_uris = value;
            }
            if ("__query_col_name__".equals(key)) {
                qcol = value;
            }
            if (key.startsWith("__pred_")) {
                qpred.add(value);
            }
        }
        query = query.replace(";", "");
        query = query.replace("*", qcol);
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
