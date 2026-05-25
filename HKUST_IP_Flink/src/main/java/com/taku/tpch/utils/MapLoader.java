package com.taku.tpch.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class MapLoader {
    public static void main(String[] args) throws IOException {
        String basePath = "file:///home/taku/IP/data/";
        // Map<Integer, Double> lineitemMap = loadLineitemStatic(basePath + "lineitem.csv");
    }

    public static Map<Integer, String> loadCustomerStatic(String path) throws IOException {
        Map<Integer, String> map = new HashMap<>();
        java.nio.file.Path filePath = Paths.get(path.replace("file://", ""));

        Files.lines(filePath).forEach(line -> {
            String[] vals = line.split("\\|");
            if (vals.length > 6) {
                // key: custKey, value: mktSegment
                map.put(Integer.parseInt(vals[0]), vals[6]);
            }
        });
        System.out.println("Customer table mapping has loaded " + map.size() + " pairs");
        return map;
    }


    public static Map<Integer, Double> loadLineitemStatic(String path) throws IOException {
        Map<Integer, Double> map = new HashMap<>();
        java.nio.file.Path filePath = Paths.get(path.replace("file://", "")); // 预设初始大小减少扩容开销

        Files.lines(filePath).forEach(line -> {
            String[] vals = line.split("\\|");
            int orderKey = Integer.parseInt(vals[0]);
            double extendedPrice = Double.parseDouble(vals[5]);
            double discount = Double.parseDouble(vals[6]);
            double itemRevenue = extendedPrice * (1 - discount);
            // key: orderKey, value: totalRevenue
            map.merge(orderKey, itemRevenue, Double::sum);
        });
        System.out.println("LineItem table mapping has loaded " + map.size() + " pairs");
        return map;
    }
}