package com.taku.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.legacy.io.TextInputFormat;
import org.apache.flink.util.Collector;


// 批处理
public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        String path = "file:///home/taku/IP/HKUST_IP_Flink/src/test/resources/words.txt";

        DataStream<String> ds = env.readFile(
                new TextInputFormat(new Path(path)),
                path
        );

        DataStream<Tuple2<String, Integer>> res = ds.flatMap(new MyFlatMapper())
                .keyBy(t -> t.f0)
                .sum(1);

        res.print();


        System.out.println("--- Job Starting ---");
        env.execute();
        System.out.println("--- Job Finished ---");
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word: words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
