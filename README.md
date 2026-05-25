### Notice (Data Modification & Setup)

The `data/` folder included in this GitHub repository contains only **truncated, low-sample data** designed solely to verify that the project builds and executes without errors. To reproduce the benchmark results detailed in the report or to conduct valid performance stress tests, you must replace this data and update the configuration as follows:

1. **Acquire Standard TPC-H Data**:
   Use the official TPC-H benchmark tool (`dbgen`) to generate data at **Scale Factor = 1 (SF=1, approx. 1GB)** or higher (consisting of `customer.tbl`, `orders.tbl`, and `lineitem.tbl`). Ensure they are converted to `.csv` format and that fields remain separated by the standard `|` delimiter.

2. **Update to Absolute Paths**:
   Open `MapLoader.java` and the main streaming job entry files. Locate the data path definitions and rewrite the `basePath` variable to match the **absolute local path** on your machine.
   ```java
   // Example: Modify this to point to your local absolute directory
   String basePath = "file:///your/local/absolute/path/to/data/";
3. **Enable External Union & Pre-Sorting (Code Uncommenting Guide)**:
   Uncomment the relevant processing blocks in your execution logic if `sorted_orders.csv`, `sorted_lineitem.csv` or `merged_final.csv` does not exist,
