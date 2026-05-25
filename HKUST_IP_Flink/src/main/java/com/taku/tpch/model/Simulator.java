package com.taku.tpch.model;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import static com.taku.tpch.utils.DateParser.parseDate;

public class Simulator {
    // Streaming Simulator Process (for parallelism 1 only)
    public static class StreamSimulator<T> extends ProcessFunction<T, T> {
        private final double speedUp; // 倍速，如 1000.0
        private transient long firstEventTs = 0L;
        private transient long startRealTime = 0L;

        public StreamSimulator(double speedUp) {
            this.speedUp = speedUp;
        }

        @Override
        public void processElement(T value, ProcessFunction<T, T>.Context ctx, Collector<T> out) throws Exception {
            long currentEventTs = ctx.timestamp();

            // 1. Initialize when the first element is processed
            if (firstEventTs == 0L) {
                firstEventTs = currentEventTs;
                startRealTime = System.currentTimeMillis();
            }


            // 2. How long has business date passed
            long businessDelay = currentEventTs - firstEventTs;

            // 3. Corresponding real time passed
            // Real Time Interval = Business Time Interval / speedUp
            long realDelay = (long) (businessDelay / speedUp);

            // 4. How long has real time passed
            long realElapsed = System.currentTimeMillis() - startRealTime;

            // 5. Thread sleep if current element arrives too early
            long sleepTime = realDelay - realElapsed;

            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }

            // Output
            out.collect(value);
        }
    }


    // Parallel Streaming Simulator Process
    public static class ParallelOrderStreamSimulator<T> extends ProcessFunction<T, T> {
        private final double speedUp;
        private final long globalFirstEventTs = parseDate("1992-01-01");
        private transient long startRealTime = 0L;

        public ParallelOrderStreamSimulator(double speedUp) {
            this.speedUp = speedUp;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            startRealTime = System.currentTimeMillis();
        }

        @Override
        public void processElement(T value, ProcessFunction<T, T>.Context ctx, Collector<T> out) throws Exception {
            MyEvent.Order order = (MyEvent.Order) value;
            long currentEventTs = parseDate(order.orderDate);

            long businessDelay = currentEventTs - globalFirstEventTs;

            if (businessDelay < 0) {
                out.collect(value);
                return;
            }

            long sleepTime = startRealTime + (long) (businessDelay / speedUp) - System.currentTimeMillis();

            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }

            out.collect(value);
        }
    }


    public static class ParallelLineitemStreamSimulator<T> extends ProcessFunction<T, T> {
        private final double speedUp;
        private final long globalFirstEventTs = parseDate("1992-01-01");
        private transient long startRealTime = 0L;

        public ParallelLineitemStreamSimulator(double speedUp) {
            this.speedUp = speedUp;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            startRealTime = System.currentTimeMillis();
        }

        @Override
        public void processElement(T value, ProcessFunction<T, T>.Context ctx, Collector<T> out) throws Exception {
            MyEvent.LineItem lineitem = (MyEvent.LineItem) value;
            long currentEventTs = parseDate(lineitem.shipDate);

            long businessDelay = currentEventTs - globalFirstEventTs;

            if (businessDelay < 0) {
                out.collect(value);
                return;
            }

            long sleepTime = startRealTime + (long) (businessDelay / speedUp) - System.currentTimeMillis();

            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }

            out.collect(value);
        }
    }


    public static class ParallelBusinessStreamSimulator<T> extends ProcessFunction<T, T> {
        private final double speedUp;
        private final long globalFirstEventTs = parseDate("1992-01-01");
        private transient long startRealTime = 0L;

        public ParallelBusinessStreamSimulator(double speedUp) {
            this.speedUp = speedUp;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            startRealTime = System.currentTimeMillis();
        }

        @Override
        public void processElement(T value, ProcessFunction<T, T>.Context ctx, Collector<T> out) throws Exception {
            MyEvent.BusinessEvent be = (MyEvent.BusinessEvent) value;
            long currentEventTs = parseDate(be.date);

            long businessDelay = currentEventTs - globalFirstEventTs;

            if (businessDelay < 0) {
                out.collect(value);
                return;
            }

            long sleepTime = startRealTime + (long) (businessDelay / speedUp) - System.currentTimeMillis();

            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }

            out.collect(value);
        }
    }


    public static class ParallelMergedBusinessStreamSimulator<T> extends ProcessFunction<T, T> {
        private final double speedUp;
        private final long globalFirstEventTs = parseDate("1992-01-01");
        private transient long startRealTime = 0L;

        public ParallelMergedBusinessStreamSimulator(double speedUp) {
            this.speedUp = speedUp;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            startRealTime = System.currentTimeMillis();
        }

        @Override
        public void processElement(T value, ProcessFunction<T, T>.Context ctx, Collector<T> out) throws Exception {
            MyEvent.MergedBusinessEvent be = (MyEvent.MergedBusinessEvent) value;
            long currentEventTs = parseDate(be.date);

            long businessDelay = currentEventTs - globalFirstEventTs;

            if (businessDelay < 0) {
                out.collect(value);
                return;
            }

            long sleepTime = startRealTime + (long) (businessDelay / speedUp) - System.currentTimeMillis();

            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }

            out.collect(value);
        }
    }
}