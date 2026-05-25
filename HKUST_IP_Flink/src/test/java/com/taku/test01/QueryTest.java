package com.taku.test01;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class QueryTest {
    public static void main(String[] args) {
        // 1. Initialize Flink SQL environment (version 2.2.0)
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 2. Create table from csv files
        // File path
        String filePath = "file:///home/taku/IP/data/orders.csv";

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
                        "  'path' = '" + filePath + "'," +
                        "  'format' = 'csv'," +
                        "  'csv.field-delimiter' = '|'," +
                        "  'csv.ignore-parse-errors' = 'true'" +
                        ")"
        );

        // 3. Process the test query: get the top 5 sales revenue grouped by date
        System.out.println(">>> Processing TPC-H Test Query <<<");

        tEnv.executeSql(
                "SELECT " +
                        "  o_orderdate, " +
                        "  COUNT(*) as order_count, " +
                        "  SUM(o_totalprice) as daily_sum " +
                        "FROM orders " +
                        "GROUP BY o_orderdate " +
                        "ORDER BY daily_sum DESC " +
                        "LIMIT 5"
        ).print();
    }
}
