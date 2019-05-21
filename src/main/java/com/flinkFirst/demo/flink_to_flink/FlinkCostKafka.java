package com.flinkFirst.demo.flink_to_flink;

import com.flinkFirst.demo.MySQLSink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FlinkCostKafka {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);


//        DataStream<Tuple2<String, Integer>> counts = null;


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
        properties.setProperty("group.id", "flink-student");

        FlinkKafkaConsumer08<String> myConsumer = new FlinkKafkaConsumer08<String>("flink-student", new SimpleStringSchema(),
                properties);

        DataStream<String> stream = env.addSource(myConsumer);

        SingleOutputStreamOperator<String> boys;
        SingleOutputStreamOperator<String> girls;

        //counts= stream.flatMap(new LineSplitter()).keyBy(0).window(ProcessingTimeSessionWindows.withGap(Time.seconds(2))).sum(1);

        //counts.addSink(new MySQLSink());

        boys=stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String input) throws Exception {
                String[] tokens = input.toLowerCase().split(",");
                return tokens[1].equals("boy");
            }
        });

        girls=stream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String input) throws Exception {
                String[] tokens = input.toLowerCase().split(",");
                return tokens[1].equals("girl");
            }
        });


        Properties propertiesBoy = new Properties();
        propertiesBoy.setProperty("bootstrap.servers", "localhost:9092");
        propertiesBoy.setProperty("zookeeper.connect", "localhost:2181");
        propertiesBoy.setProperty("group.id", "student-boys");

        boys.addSink(new FlinkKafkaProducer08<String>(
                "localhost:9092",
                "flink-boys",
                new org.apache.flink.api.common.serialization.SimpleStringSchema()
        ));

        girls.addSink(new FlinkKafkaProducer08<String>(
                "localhost:9092",
                "flink-girls",
                new org.apache.flink.api.common.serialization.SimpleStringSchema()
        ));



        env.execute("FlinkCostKafka");
    }

//    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
//        private static final long serialVersionUID = 1L;
//
//        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
//            String[] tokens = value.toLowerCase().split("\\W+");
//            for (String token : tokens) {
//                if (token.length() > 0) {
//                    out.collect(new Tuple2<String, Integer>(token, 1));
//                }
//            }
//        }
//    }

}