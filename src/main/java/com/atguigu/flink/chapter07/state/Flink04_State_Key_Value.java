package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/8/5 15:11
 */
public class Flink04_State_Key_Value {
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        
        //
        env
            .socketTextStream("hadoop162", 9999) // socket只能是1
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(
                    data[0],
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2])
                );
            })
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                
                private ValueState<Integer> lastVcState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 获取键控状态
                    lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Integer.class));
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    
                    // 获取状态中的值
                    // 如果状态没值, 会返回null
                    Integer lastVc = lastVcState.value();
                    System.out.println(lastVc);
    
                    if (lastVc != null) {
                        System.out.println(lastVc + "  " + value.getVc());
                        if (value.getVc() > 10 && lastVc > 10) {
                            out.collect(ctx.getCurrentKey() + " 连续两次超过10,发出红色预警...");
                        }
                    }
                    
                    lastVcState.update(value.getVc());
                    
                }
            })
            .print();
        
        
        env.execute();
    }
}
