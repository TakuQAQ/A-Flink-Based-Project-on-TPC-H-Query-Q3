package com.taku.tpch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class Query03_SQL {
    public static void main(String[] args) {
        // 1. Initialize Flink SQL environment (version 2.2.0)
        int parallelism = 4;

        Configuration conf = new Configuration();
        conf.setString("table.exec.resource.default-parallelism", String.valueOf(parallelism));

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .withConfiguration(conf)
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 2. Create table from csv files
        // File path
        String basePath = "file:///home/taku/IP/data/";

        // Customer table
        tEnv.executeSql(
                "CREATE TABLE customer (" +
                        "  c_custkey     INT," +
                        "  c_name        STRING," +
                        "  c_address     STRING," +
                        "  c_nationkey   INT," +
                        "  c_phone       STRING," +
                        "  c_acctbal     DECIMAL(15, 2)," +
                        "  c_mktsegment  STRING," +
                        "  c_comment     STRING" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = 'file:///home/taku/IP/data/customer.csv'," +
                        "  'format' = 'csv'," +
                        "  'csv.field-delimiter' = '|'," +
                        "  'csv.ignore-parse-errors' = 'true'" +
                        ")"
        );

        // Orders table
        tEnv.executeSql(
                "CREATE TABLE orders (" +
                        "  o_orderkey      INT," +
                        "  o_custkey       INT," +
                        "  o_orderstatus   STRING," +
                        "  o_totalprice    DECIMAL(15, 2)," +
                        "  o_orderdate     DATE," +
                        "  o_orderpriority STRING," +
                        "  o_clerk         STRING," +
                        "  o_shippriority  INT," +
                        "  o_comment       STRING" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = 'file:///home/taku/IP/data/orders.csv'," +
                        "  'format' = 'csv'," +
                        "  'csv.field-delimiter' = '|'," +
                        "  'csv.ignore-parse-errors' = 'true'" +
                        ")"
        );

        // Lineitem table
        tEnv.executeSql(
                "CREATE TABLE lineitem (" +
                        "  l_orderkey      INT," +
                        "  l_partkey       INT," +
                        "  l_suppkey       INT," +
                        "  l_linenumber    INT," +
                        "  l_quantity      DECIMAL(15, 2)," +
                        "  l_extendedprice DECIMAL(15, 2)," +
                        "  l_discount      DECIMAL(15, 2)," +
                        "  l_tax           DECIMAL(15, 2)," +
                        "  l_returnflag    STRING," +
                        "  l_linestatus    STRING," +
                        "  l_shipdate      DATE," +
                        "  l_commitdate    DATE," +
                        "  l_receiptdate   DATE," +
                        "  l_shipinstruct  STRING," +
                        "  l_shipmode      STRING," +
                        "  l_comment       STRING" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = 'file:///home/taku/IP/data/lineitem.csv'," +
                        "  'format' = 'csv'," +
                        "  'csv.field-delimiter' = '|'," +
                        "  'csv.ignore-parse-errors' = 'true'" +
                        ")"
        );

        // --- 3. Process Query 3 ---
        // Default setting: Segment = 'BUILDING', Date = '1995-03-15'

        System.out.println(">>> Processing TPC-H Q3 (Shipping Priority Query) <<<");

        long startTime = System.currentTimeMillis();

        tEnv.executeSql(
                "SELECT " +
                        "  l.l_orderkey, " +
                        "  SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, " +
                        "  o.o_orderdate, " +
                        "  o.o_shippriority " +
                        "FROM " +
                        "  customer c, " +
                        "  orders o, " +
                        "  lineitem l " +
                        "WHERE " +
                        "  c.c_mktsegment = 'BUILDING' " +           // Filter 1
                        "  AND c.c_custkey = o.o_custkey " +         // Join 1
                        "  AND l.l_orderkey = o.o_orderkey " +       // Join 2
                        "  AND o.o_orderdate < DATE '1995-03-15' " + // Filter 2
                        "  AND l.l_shipdate > DATE '1995-03-15' " +  // Filter 3
                        "GROUP BY " +
                        "  l.l_orderkey, " +
                        "  o.o_orderdate, " +
                        "  o.o_shippriority " +
                        "ORDER BY " +
                        "  revenue DESC, " +
                        "  o.o_orderdate " +
                        "LIMIT 10"                                   // Select top 10
        ).print();

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;

        // Time cost
        System.out.println(duration + " seconds");
    }
}