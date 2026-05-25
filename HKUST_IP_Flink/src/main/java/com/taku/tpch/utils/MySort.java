package com.taku.tpch.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MySort {
    public static void main(String[] args) {
        csvSort("orders.csv", "/home/taku/IP/data/", new int[]{4});
    }

    public static void csvSort(String fileName, String basePath, int[] idx) {
        String inputPath = basePath + fileName;
        String outputPath = basePath + "sorted_" + fileName;
        String delimiter = "\\|";

        List<String> lines = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputPath), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    lines.add(line);
                }
            }
        } catch (Exception ex) {
            System.err.println("Read Error: " + ex.getMessage());
            return;
        }

        lines.sort((l1, l2) -> {
            String[] l1_val = l1.split(delimiter);
            String[] l2_val = l2.split(delimiter);
            for (int i : idx) {
                String a = (i < l1_val.length) ? l1_val[i] : "";
                String b = (i < l2_val.length) ? l2_val[i] : "";
                int c = a.compareTo(b);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        });

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath), StandardCharsets.UTF_8))) {
            for (String sortedLine : lines) {
                bw.write(sortedLine);
                bw.newLine();
            }
            bw.flush();
        } catch (Exception ex) {
            System.err.println("Write Error: " + ex.getMessage());
            return;
        }

        System.out.println("Sort Complete!");
    }
}
