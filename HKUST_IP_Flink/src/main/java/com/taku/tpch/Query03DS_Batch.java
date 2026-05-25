package com.taku.tpch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

import com.taku.tpch.model.MyEvent.*;


public class Query03DS_Batch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. BATCH mode
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(4);


        // 2. Read Data
        String basePath = "file:///home/taku/IP/data/";
        // ** Substitution **
        String segment = "BUILDING";
        String th_date = "1995-03-15";

        // Customer
        FileSource<String> customerSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path(basePath + "customer.csv")
        ).build();

        DataStream<String> customerRaw = env.fromSource(
                customerSource,
                WatermarkStrategy.noWatermarks(),
                "customer-source"
        );

        DataStream<Customer> customerDS = customerRaw
                .map(line -> {
                    String[] vals = line.split("\\|");
                    return new Customer(Integer.parseInt(vals[0]), vals[6]);
                })
                .filter(c -> segment.equals(c.mktSegment));

        // customerDS.print("Customer");

        // Orders
        FileSource<String> orderSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path(basePath + "orders.csv")
        ).build();

        DataStream<String> orderRaw = env.fromSource(
                orderSource,
                WatermarkStrategy.noWatermarks(),
                "order-source"
        );

        DataStream<Order> orderDS = orderRaw
                .map(line -> {
                    String[] vals = line.split("\\|");
                    return new Order(Integer.parseInt(vals[0]), Integer.parseInt(vals[1]), vals[4], Integer.parseInt(vals[7]));
                })
                .filter(o -> o.orderDate.compareTo(th_date) < 0);

        // orderDS.print("Order");

        // LineItem
        FileSource<String> lineitemSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path(basePath + "lineitem.csv")
        ).build();

        DataStream<String> lineitemRaw = env.fromSource(
                lineitemSource,
                WatermarkStrategy.noWatermarks(),
                "lineitem-source"
        );

        DataStream<LineItem> lineitemDS = lineitemRaw
                .map(line -> {
                    String[] vals = line.split("\\|");
                    return new LineItem(Integer.parseInt(vals[0]), Double.parseDouble(vals[5]), Double.parseDouble(vals[6]), vals[10]);
                })
                .filter(l -> l.shipDate.compareTo(th_date) > 0);

        // lineitemDS.print("LineItem");


        // Join 1
        DataStream<FilteredOrder> joinedCO = customerDS
                .coGroup(orderDS)
                .where(c -> c.custKey)
                .equalTo(o -> o.custKey)
                .window(GlobalWindows.create())
                .trigger(new BatchTrigger<>())
                .apply((Iterable<Customer> customers, Iterable<Order> orders, Collector<FilteredOrder> out) -> {
                    if (customers.iterator().hasNext()) {
                        for (Order o : orders) {
                            out.collect(new FilteredOrder(o.orderKey, o.orderDate, o.shipPriority));
                        }
                    }
                }, TypeInformation.of(FilteredOrder.class));

        // joinedCO.print("JOIN1");


        // Join 2: (Customer+Orders) Join LineItem
        DataStream<Q3Result> finalDS = joinedCO
                .coGroup(lineitemDS)
                .where(co -> co.orderKey)
                .equalTo(l -> l.orderKey)
                .window(GlobalWindows.create())
                .trigger(new BatchTrigger<>())
                .apply((Iterable<FilteredOrder> cos, Iterable<LineItem> items, Collector<Q3Result> out) -> {
                    if (cos.iterator().hasNext()) {
                        FilteredOrder order = cos.iterator().next();
                        double sum = 0;
                        for (LineItem item : items) {
                            // revenue = extendedprice * (1 - discount)
                            sum += item.extendedPrice * (1 - item.discount);
                        }
                        if (sum > 0) {
                            out.collect(new Q3Result(order.orderKey, sum, order.orderDate, order.shipPriority));
                        }
                    }
                }, TypeInformation.of(Q3Result.class));

        // finalDS.print("JOIN2");


        // Sorting
        finalDS
                .windowAll(GlobalWindows.create())
                .trigger(new BatchTrigger<>())
                .apply((GlobalWindow window, Iterable<Q3Result> values, Collector<String> out) -> {
                    ArrayList<Q3Result> list = new ArrayList<>();
                    for (Q3Result r : values) {
                        list.add(r);
                    }
                    list.sort((a, b) -> Double.compare(b.revenue, a.revenue));
                    for (int i = 0; i < Math.min(10, list.size()); i++) {
                        Q3Result res = list.get(i);
                        String str = String.format("Rank %2d | orderKey: %8d | Revenue: %10.2f | orderDate: %s",
                                i + 1, res.orderKey, res.revenue, res.orderDate);
                        out.collect(str);
                    }
                }, Types.STRING)
                .print("Final-Q3-Top10")
                .setParallelism(1);


        long startTime = System.currentTimeMillis();

        env.execute("Check Data");

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;

        System.out.println(duration + "seconds");
    }


    // Trigger
    public static class BatchTrigger<T> extends Trigger<T, GlobalWindow> {

        @Override
        public TriggerResult onElement(T element, long timestamp, GlobalWindow window, TriggerContext ctx) {
            ctx.registerEventTimeTimer(Long.MAX_VALUE);
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) {
            // Trigger when all data is read
            return (time == Long.MAX_VALUE) ? TriggerResult.FIRE : TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) {
            ctx.deleteEventTimeTimer(Long.MAX_VALUE);
        }
    }
}