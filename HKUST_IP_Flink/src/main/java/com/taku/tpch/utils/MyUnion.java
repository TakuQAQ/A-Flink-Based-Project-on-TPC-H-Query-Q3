package com.taku.tpch.utils;

import java.io.*;
import java.util.*;

public class MyUnion {
    public static void main(String[] args) throws IOException {
        String basePath = "/home/taku/IP/data/";
        mergeAndSort(basePath + "sorted_orders.csv", basePath + "sorted_lineitem.csv", basePath + "merged_final.csv");
    }


    public static class Record {
        Integer orderKey;
        Integer custKey;
        Double amount;
        String date;
        boolean isOrder;

        public Record(Integer orderKey, Integer custKey, Double amount, String date, boolean isOrder) {
            this.orderKey = orderKey;
            this.custKey = custKey;
            this.amount = amount;
            this.date = date;
            this.isOrder = isOrder;
        }
    }


    public static void mergeAndSort(String orderFilePath, String lineitemFilePath, String outputFilePath) throws IOException {
        List<Record> allRecords = new ArrayList<>();

        System.out.println(">>> Loading Orders...");
        Map<Integer, Integer> orderToCustMap = loadOrders(orderFilePath, allRecords);

        System.out.println(">>> Loading Lineitem...");
        loadLineitems(lineitemFilePath, allRecords, orderToCustMap);

        System.out.println(">>> Loading Completed ---");

        // 排序规则：date ASC, isOrder DESC
        allRecords.sort((r1, r2) -> {
            int dateCmp = r1.date.compareTo(r2.date);
            if (dateCmp != 0) {
                return dateCmp;
            }
            // 同一日期下，Order(true) 排在前面
            return Boolean.compare(r2.isOrder, r1.isOrder);
        });

        System.out.println(">>> Writing to csv file...");
        writeToCsv(outputFilePath, allRecords);
        System.out.println(">>> Complete! ---");
    }

    public static Map<Integer, Integer> loadOrders(String path, List<Record> list) throws IOException {
        String basePath = "file:///home/taku/IP/data/";
        Map<Integer, Double> orderRevenue = MapLoader.loadLineitemStatic(basePath + "lineitem.csv");
        String delimiter = "\\|";

        Map<Integer, Integer> orderToCustMap = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] cols = line.split(delimiter);
                int orderKey = Integer.parseInt(cols[0].trim());
                int custKey = Integer.parseInt(cols[1].trim());
                String date = cols[4].trim();

                Double amount = orderRevenue.getOrDefault(orderKey, 0.0);

                list.add(new Record(orderKey, custKey, amount, date, true));

                orderToCustMap.put(orderKey, custKey);
            }
        } catch (Exception e) {
            System.err.println("Loading Order Failed: " + e.getMessage());
        }

        return orderToCustMap;
    }

    public static void loadLineitems(String path, List<Record> list, Map<Integer, Integer> orderToCustMap) {
        String delimiter = "\\|";

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] cols = line.split(delimiter);
                int orderKey = Integer.parseInt(cols[0].trim());
                double price = Double.parseDouble(cols[5].trim());
                double discount = Double.parseDouble(cols[6].trim());
                String date = cols[10].trim();

                // 计算公式：amount = extendedPrice * (1 - discount)
                double amount = price * (1.0 - discount);

                list.add(new Record(orderKey, orderToCustMap.get(orderKey), -amount, date, false));
            }
        } catch (Exception e) {
            System.err.println("Loading Lineitem Failed: " + e.getMessage());
        }
    }

    public static void writeToCsv(String path, List<Record> list) {
        String delimiter = "|";

        try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(path)))) {
            for (Record r : list) {
                // 使用 StringBuilder 提高大规模拼接效率
                StringBuilder sb = new StringBuilder();
                sb.append(r.orderKey).append(delimiter)
                        .append(r.custKey).append(delimiter)
                        .append(String.format("%.2f", r.amount)).append(delimiter)
                        .append(r.date).append(delimiter)
                        .append(r.isOrder);
                pw.println(sb.toString());
            }
        } catch (Exception e) {
            System.err.println("Writing Failed: " + e.getMessage());
        }
    }
}