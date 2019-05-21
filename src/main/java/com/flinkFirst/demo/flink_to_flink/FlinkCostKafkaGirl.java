package com.flinkFirst.demo.flink_to_flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FlinkCostKafkaGirl {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
        properties.setProperty("group.id", "flink-girls");

        FlinkKafkaConsumer08<String> myConsumer = new FlinkKafkaConsumer08<String>("flink-girls", new SimpleStringSchema(),
                properties);

        DataStream<String> stream = env.addSource(myConsumer);
        DataStream<String> youthStream;
        DataStream<String> oldStream;
        SingleOutputStreamOperator<Collector<Tuple3<String, String, Integer>>> youth = null;
        DataStream<Tuple3<String,String,Integer>> countsYouth = null;
        DataStream<Tuple3<String,String,Integer>> countsOld = null;

        youthStream=stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String input) throws Exception {
                String[] tokens = input.toLowerCase().split(",");
                return Integer.parseInt(tokens[3])>2000;
            }
        });

        oldStream=stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String input) throws Exception {
                String[] tokens = input.toLowerCase().split(",");
                return Integer.parseInt(tokens[3])<2000;
            }
        });

        countsYouth=youthStream.flatMap(new YouthSplitter()).keyBy(1).window(ProcessingTimeSessionWindows.withGap(Time.seconds(2))).sum(2);
        countsOld=oldStream.flatMap(new OldSplitter()).keyBy(1).window(ProcessingTimeSessionWindows.withGap(Time.seconds(2))).sum(2);
        countsYouth.print();
        countsOld.print();
        env.execute("FlinkCostKafkaBoy");
    }

    public static final class YouthSplitter implements FlatMapFunction<String, Tuple3<String,String,Integer>> {
        private static final long serialVersionUID = 1L;

        public void flatMap(String value, Collector<Tuple3<String,String, Integer>> out) {
                    out.collect(new Tuple3<String,String, Integer>("girl","youth",1));
        }
    }

    public static final class OldSplitter implements FlatMapFunction<String, Tuple3<String,String,Integer>> {
        private static final long serialVersionUID = 1L;

        public void flatMap(String value, Collector<Tuple3<String,String, Integer>> out) {
            out.collect(new Tuple3<String,String, Integer>("girl","old",1));
        }
    }
}