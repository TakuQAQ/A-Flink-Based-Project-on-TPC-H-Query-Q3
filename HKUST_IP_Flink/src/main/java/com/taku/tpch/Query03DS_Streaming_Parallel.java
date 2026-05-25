package com.taku.tpch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import java.util.*;

import org.apache.flink.util.Collector;

import com.taku.tpch.model.MyEvent;
import static com.taku.tpch.model.Simulator.ParallelOrderStreamSimulator;
import static com.taku.tpch.model.Simulator.ParallelLineitemStreamSimulator;
import static com.taku.tpch.model.Simulator.ParallelBusinessStreamSimulator;
import com.taku.tpch.utils.MapLoader;
import static com.taku.tpch.utils.DateParser.parseDate;


public class Query03DS_Streaming_Parallel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. STREAMING mode
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(4);


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


        int n = 8;
        DataStream<String> top10Result = eventStream
                // Local sort
                .keyBy(event -> event.orderKey % n)
                .process(new LocalTop10Processor())
                .setParallelism(n)
                // Global sort
                .keyBy(event -> "G")
                .process(new GlobalTop10Processor())
                .setParallelism(1);


        top10Result.print().setParallelism(1);


        /*
        DataStream<Tuple3<String, Integer, List<Tuple3<Integer, Double, String>>>> top10ResultLocal = eventStream
                .keyBy(event -> event.orderKey % n)
                .process(new LocalTop10Processor())
                .setParallelism(n);


        top10ResultLocal.print().setParallelism(1);
         */


        env.execute("Check Data");

    }


    public static class LocalTop10Processor extends KeyedProcessFunction<Integer, MyEvent.BusinessEvent, Tuple3<String, Integer, List<Tuple3<Integer, Double, String>>>> {

        private transient MapState<Integer, Tuple3<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>> localUnshippedState;
        private transient MapState<Integer, String> orderDateMap;
        private transient boolean timerStarted = false;
        private final int interval = 5000;

        @Override
        public void open(OpenContext openContext) {
            localUnshippedState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("local_unshipped",
                            TypeInformation.of(Integer.class),
                            TypeInformation.of(new TypeHint<Tuple3<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>>(){})));
            orderDateMap = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("order_date_map", Integer.class, String.class));
        }

        @Override
        public void processElement(MyEvent.BusinessEvent event, Context ctx, Collector<Tuple3<String, Integer, List<Tuple3<Integer, Double, String>>>> out) throws Exception {
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
            Tuple3<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>> historyState = localUnshippedState.get(event.orderKey);

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
            localUnshippedState.put(event.orderKey, historyState);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, Integer, List<Tuple3<Integer, Double, String>>>> out) throws Exception {
            localRefreshTop10(ctx, out);

            long nxt = ctx.timerService().currentProcessingTime() + interval;
            ctx.timerService().registerProcessingTimeTimer(nxt);
        }

        private void localRefreshTop10(OnTimerContext ctx, Collector<Tuple3<String, Integer, List<Tuple3<Integer, Double, String>>>> out) throws Exception {
            long threshold = ctx.timerService().currentWatermark();
            List<Tuple3<Integer, Double, String>> list = new ArrayList<>();

            for (Map.Entry<Integer, Tuple3<Tuple2<Double, Long>, Tuple2<Double, Long>, Tuple2<Double, Long>>> entry : localUnshippedState.entries()) {
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
                    list.add(Tuple3.of(entry.getKey(), val, orderDateMap.get(entry.getKey())));
                }
            }

            // Local sort
            list.sort((a, b) -> b.f1.compareTo(a.f1));

            // Collect top 10
            List<Tuple3<Integer, Double, String>> localResult = list.subList(0, Math.min(10, list.size()));

            // Local output
            SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            String simulatedDate = sdf.format(new Date(threshold));
            int localIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();

            // System.out.println(simulatedDate + " " + localIndex + " SENT");
            out.collect(Tuple3.of(simulatedDate, localIndex, new ArrayList<>(localResult)));
        }
    }


    public static class GlobalTop10Processor extends KeyedProcessFunction<String, Tuple3<String, Integer, List<Tuple3<Integer, Double, String>>>, String> {

        // Key: subtaskIndex, Value: local top 10
        private transient MapState<Integer, List<Tuple3<Integer, Double, String>>> localResults;
        private transient ValueState<Integer> count;
        private final int partitionNum = 4;

        @Override
        public void open(OpenContext ctx) {
            localResults = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                    "local_results", Integer.class, (Class<List<Tuple3<Integer, Double, String>>>)(Class<?>)List.class));
            count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Integer.class));
        }

        @Override
        public void processElement(Tuple3<String, Integer, List<Tuple3<Integer, Double, String>>> input, Context ctx, Collector<String> out) throws Exception {
            String simulatedDate = input.f0;
            int localIndex = input.f1;
            List<Tuple3<Integer, Double, String>> localList = input.f2;

            if (!localResults.contains(localIndex)) {
                Integer currentCount = count.value();
                count.update((currentCount == null ? 0 : currentCount) + 1);
            }
            localResults.put(localIndex, localList);


            if (count.value() >= partitionNum) {
                // Global sort
                List<Tuple3<Integer, Double, String>> candidates = new ArrayList<>();
                for (List<Tuple3<Integer, Double, String>> list : localResults.values()) {
                    candidates.addAll(list);
                }
                candidates.sort((a, b) -> b.f1.compareTo(a.f1));

                // Top 10 print
                System.out.println("\n--- [TOP 10] REFRESH TIME: " + new Date() + " ---");
                System.out.println("CURRENT SIMULATED DATE: " + simulatedDate);

                for (int i = 0; i < Math.min(10, candidates.size()); i++) {
                    Tuple3<Integer, Double, String> t = candidates.get(i);
                    System.out.printf("RANK %d | OrderKey: %d | Unshipped Rev: %.2f | OrderDate: %s\n",
                            i + 1, t.f0, t.f1, t.f2);
                }

                // State clear
                count.clear();
                localResults.clear();
            }
        }
    }
}