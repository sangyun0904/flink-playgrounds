package com.example;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
    private ValueState<Boolean> blocked;

    @Override
    public void flatMap1(String data_value, Collector<String> out) throws Exception {
        System.out.println("flatmap 1 data_value : " + data_value);
        if (blocked.value() == null) {
            out.collect(data_value);
            blocked.update(Boolean.TRUE);
        }
    }

    @Override
    public void flatMap2(String data_value, Collector<String> out) throws Exception {
        System.out.println("flatmap 2 data_value : " + data_value);
        if (blocked.value() == null) {
            out.collect(data_value);
            blocked.update(Boolean.TRUE);
        }
    }

    @Override
    public void open(Configuration config) {
        blocked = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("blocked", Boolean.class));
    }
}
