package com.taku.tpch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.taku.tpch.model.MyEvent;
import static com.taku.tpch.model.Simulator.ParallelOrderStreamSimulator;
import static com.taku.tpch.model.Simulator.ParallelLineitemStreamSimulator;
import static com.taku.tpch.model.Simulator.ParallelBusinessStreamSimulator;
import com.taku.tpch.utils.MapLoader;
import static com.taku.tpch.utils.DateParser.parseDate;


public class Query03DS_Streaming {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. STREAMING mode
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(2);


        // 2. Read data
        String basePath = "file:///home/taku/IP/data/";
        // ** Substitution **
        String segment = "BUILDING";
        double speed = 1000000.0;

        // Customer broadcasted mapping
        Map<Integer, String> customerMap = MapLoader.loadCustomerStatic(basePath + "customer.csv");

        // Orders stream
        // ***** Run the next line if sorted_orders.csv does not exist *****
        // MySort.csvSort("orders.csv", "/home/taku/IP/data/", new int[]{4});

        FileSource<String> orderSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path(basePath + "sorted_orders.csv")
        ).build();

        DataStream<String> orderRaw = env.fromSource(
                orderSource,
                WatermarkStrategy.noWatermarks(),
                "order-source"
        ).setParallelism(1);

        DataStream<MyEvent.Order> orderDS = orderRaw
                .map(line -> {
                    String[] vals = line.split("\\|");
                    return new MyEvent.Order(Integer.parseInt(vals[0]), Integer.parseInt(vals[1]), vals[4], Integer.parseInt(vals[7]));
                })
                .filter(o -> segment.equals(customerMap.get(o.custKey)))
                .setParallelism(1)
                .rebalance();

        DataStream<MyEvent.Order> simulatedOrderDS = orderDS
                .process(new ParallelOrderStreamSimulator<>(3500000.0))
                .setParallelism(8)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<MyEvent.Order>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) -> parseDate(event.orderDate))
                );


        // Simulator test
        simulatedOrderDS
                .keyBy(o -> customerMap.get(o.custKey))
                .window(TumblingEventTimeWindows.of(Duration.ofDays(1)))
                .apply(new WindowFunction<MyEvent.Order, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<MyEvent.Order> input, Collector<String> out) {
                        // Order Count by window
                        long count = 0;
                        for (MyEvent.Order o : input) {
                            count++;
                        }

                        // Business date
                        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
                        String businessDate = sdf.format(new java.util.Date(window.getStart()));

                        // Output
                        out.collect(">>> [Q3 Monitor] Business Date " + businessDate +
                                " | Segment: " + key +
                                " | New Order Increment: " + count);
                    }
                })
                // .print().setParallelism(8)
                ;


        // LineItem broadcasted mapping
        Map<Integer, Double> lineitemMap = MapLoader.loadLineitemStatic(basePath + "lineitem.csv");

        // LineItem stream
        // ***** Run the next line if sorted_lineitem.csv does not exist *****
        // MySort.csvSort("lineitem.csv", "/home/taku/IP/data/", new int[]{10});

        FileSource<String> lineitemSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path(basePath + "sorted_lineitem.csv")
        ).build();

        DataStream<String> lineitemRaw = env.fromSource(
                lineitemSource,
                WatermarkStrategy.noWatermarks(),
                "lineitem-source"
        ).setParallelism(1);

        DataStream<MyEvent.LineItem> lineitemDS = lineitemRaw
                .map(line -> {
                    String[] vals = line.split("\\|");
                    return new MyEvent.LineItem(Integer.parseInt(vals[0]), Double.parseDouble(vals[5]), Double.parseDouble(vals[6]), vals[10]);
                })
                .setParallelism(1)
                .rebalance();

        DataStream<MyEvent.LineItem> simulatedLineitemDS = lineitemDS
                .process(new ParallelLineitemStreamSimulator<>(3500000.0))
                .setParallelism(8)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<MyEvent.LineItem>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) -> parseDate(event.shipDate))
                );


        // simulatedLineitemDS.print().setParallelism(8);


        // Join
        // 1. Order stream
        DataStream<MyEvent.BusinessEvent> orderEvents = orderDS
                .map(o -> new MyEvent.BusinessEvent(o.orderKey, lineitemMap.get(o.orderKey), o.orderDate, true))
                .process(new ParallelBusinessStreamSimulator<>(speed))
                .setParallelism(1);

        // 2. Lineitem stream
        DataStream<MyEvent.BusinessEvent> lineitemEvents = lineitemDS
                .map(l -> new MyEvent.BusinessEvent(l.orderKey, -l.extendedPrice * (1 - l.discount), l.shipDate, false))
                .process(new ParallelBusinessStreamSimulator<>(speed))
                .setParallelism(1);

        // 3. Business event stream
        DataStream<MyEvent.BusinessEvent> eventStream = orderEvents
                .union(lineitemEvents)
                .process(new ParallelBusinessStreamSimulator<>(speed))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<MyEvent.BusinessEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) -> parseDate(event.date))
                );


        // eventStream.print("BE").setParallelism(8);


        DataStream<String> top10Result = eventStream
                // Global sort
                .keyBy(event -> "G")
                .process(new timerTop10Processor())
                .setParallelism(1);


        top10Result.print().setParallelism(1);


        env.execute("Check Data");

    }


    public static class timerTop10Processor extends KeyedProcessFunction<String, MyEvent.BusinessEvent, String> {

        private transient MapState<Integer, Tuple3<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>> unshippedState;
        private transient MapState<Integer, String> orderDateMap;
        private transient boolean timerStarted = false;
        private final int interval = 5000;

        @Override
        public void open(OpenContext openContext) {
            unshippedState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("unshipped",
                            TypeInformation.of(Integer.class),
                            TypeInformation.of(new TypeHint<Tuple3<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>>(){})));
            orderDateMap = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("order_date_map", Integer.class, String.class));
        }

        @Override
        public void processElement(MyEvent.BusinessEvent event, Context ctx, Collector<String> out) throws Exception {
            // Register timer
            if (!timerStarted) {
                long now = ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer(now + interval);
                timerStarted = true;
            }

            if (event.isOrder) {
                orderDateMap.put(event.orderKey, event.date);
            }

            long ts = parseDate(event.date);
            Tuple3<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>> historyState = unshippedState.get(event.orderKey);

            if (historyState == null) {
                historyState = Tuple3.of(
                        Tuple2.of(event.revenueUnshipped, ts),
                        Tuple2.of(0.0, -1L),
                        Tuple2.of(0.0, -1L)
                );
            } else {
                if (historyState.f0.f1 == ts) {
                    historyState.f0.f0 += event.revenueUnshipped;
                } else {
                    historyState.f2 = historyState.f1;
                    historyState.f1 = historyState.f0;
                    historyState.f0 = Tuple2.of(historyState.f1.f0 + event.revenueUnshipped, ts);
                }
            }
            unshippedState.put(event.orderKey, historyState);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            refreshTop10(ctx);

            long nxt = ctx.timerService().currentProcessingTime() + interval;
            ctx.timerService().registerProcessingTimeTimer(nxt);
        }

        private void refreshTop10(OnTimerContext ctx) throws Exception {
            long threshold = ctx.timerService().currentWatermark();
            List<Map.Entry<Integer, Double>> list = new ArrayList<>();

            for (Map.Entry<Integer, Tuple3<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>> entry : unshippedState.entries()) {
                Tuple3<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>> historyState = entry.getValue();
                Double val = null;

                if (historyState.f0.f1 <= threshold) {
                    val = historyState.f0.f0;
                } else if (historyState.f1.f1 != -1L && historyState.f1.f1 <= threshold) {
                    val = historyState.f1.f0;
                } else if (historyState.f2.f1 != -1L && historyState.f2.f1 <= threshold) {
                    val = historyState.f2.f0;
                }

                // Avoid floating error
                if (val != null && val > 0.01) {
                    list.add(new AbstractMap.SimpleEntry<>(entry.getKey(), val));
                }
            }

            // Global sort
            list.sort((a, b) -> b.getValue().compareTo(a.getValue()));

            // Top 10 print
            SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            String simulatedDate = sdf.format(new Date(threshold));

            System.out.println("\n--- [TOP 10] REFRESH TIME: " + new Date() + " ---");
            System.out.println("CURRENT SIMULATED DATE: " + simulatedDate);

            for (int i = 0; i < Math.min(10, list.size()); i++) {
                Map.Entry<Integer, Double> entry = list.get(i);
                System.out.printf("RANK %d | OrderKey: %d | Unshipped Rev: %.2f | OrderDate: %s\n",
                        i + 1, entry.getKey(), entry.getValue(), orderDateMap.get(entry.getKey()));
            }
        }
    }
}