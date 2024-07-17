package com.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;

import java.io.Serializable;
import java.time.Instant;

public class DataPipelinesETL {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = env
                .fromElements("DROP", "IGNORE", "HELLO", "World")
                .keyBy(String::toLowerCase);

        DataStream<String> streamOfWords = env
                .fromElements("Apache", "DROP", "Flink", "IGNORE", "HELLO", "drop", "ignore")
                .keyBy(String::toLowerCase);

        control
                .connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

}
