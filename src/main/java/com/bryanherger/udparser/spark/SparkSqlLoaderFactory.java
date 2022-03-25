package com.bryanherger.udparser.spark;

import com.vertica.sdk.*;


public class SparkSqlLoaderFactory extends ParserFactory {
    public static int MIN_ROWSET = 1;       // Min rowset value
    public static int MAX_ROWSET = 10000;   // Max rowset value
    public static int DEF_ROWSET = 100;     // Default rowset
    public static int MAX_PRELEN = 2048;    // Max predicate length
    public static int MAX_PRENUM = 10;      // Max predicate number

    @Override
    public void plan(ServerInterface srvInterface,
                     PerColumnParamReader perColumnParamReader,
                     PlanContext planCtxt) throws UdfException {
        if (!srvInterface.getParamReader().containsParameter("query")) {
            throw new UdfException(0, "Error:  SparkSqlLoader requires a 'query' string, the query to execute on the remote system");
        }
    }

    @Override
    public UDParser prepare(ServerInterface srvInterface,
                            PerColumnParamReader perColumnParamReader,
                            PlanContext planCtxt,
                            SizedColumnTypes returnType) {
        return new SparkSqlLoader();
    }

    @Override
    public void getParameterType(ServerInterface srvInterface,
                                 SizedColumnTypes parameterTypes) {
        parameterTypes.addVarchar(65000, "query");
        parameterTypes.addVarchar(65000, "partition");
        parameterTypes.addVarchar(65000, "__query_col_name__");
        parameterTypes.addVarchar(65000, "__query_col_idx__");
        for ( int k = 0 ; k < MAX_PRENUM ; k++ ) {
            parameterTypes.addVarchar(MAX_PRELEN, "__pred_"+k+"__");
        }
        parameterTypes.addInt("rowset");
        parameterTypes.addBool("src_rfilter");
        parameterTypes.addBool("src_cfilter");
        // Spark
        //.master("local[1]")
        //.config("spark.sql.warehouse.dir","hdfs:///user/spark/warehouse")
        //.config("hive.metastore.uris","thrift://ip-172-31-26-110.ec2.internal:9083")
        parameterTypes.addVarchar(65000, "spark_master");
        parameterTypes.addVarchar(65000, "spark_sql_warehouse_dir");
        parameterTypes.addVarchar(65000, "spark_hive_metastore_uris");
    }
}
