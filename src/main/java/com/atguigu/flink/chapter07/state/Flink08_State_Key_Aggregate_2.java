package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/8/5 15:11
 */
public class Flink08_State_Key_Aggregate_2 {
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
                
                
                private AggregatingState<WaterSensor, Double> vcAvgState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    // 获取键控状态
                    vcAvgState = getRuntimeContext().getAggregatingState(
                        new AggregatingStateDescriptor<WaterSensor, Tuple2<Integer, Long>, Double>(
                            "vcAvgState",
                            new AggregateFunction<WaterSensor, Tuple2<Integer, Long>, Double>() {
                                @Override
                                public Tuple2<Integer, Long> createAccumulator() {
                                    return Tuple2.of(0, 0L);
                                }
                                
                                @Override
                                public Tuple2<Integer, Long> add(WaterSensor value,
                                                                 Tuple2<Integer, Long> acc) {
                                    acc.f0 += value.getVc();
                                    acc.f1++;
                                    return acc;
                                }
                                
                                @Override
                                public Double getResult(Tuple2<Integer, Long> acc) {
                                    return acc.f0 * 1.0/acc.f1;
                                }
                                
                                @Override
                                public Tuple2<Integer, Long> merge(Tuple2<Integer, Long> a, Tuple2<Integer, Long> b) {
                                    return null;
                                }
                            },
//                            TypeInformation.of(new TypeHint<Tuple2<Integer, Long>>() {})
                            Types.TUPLE(Types.INT, Types.LONG)
                        )
                    );
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    vcAvgState.add(value);
                    
                    
                    out.collect(ctx.getCurrentKey() + " 的平均水位: " + vcAvgState.get());
                    
                }
            })
            .print();
        
        
        env.execute();
    }
    
    
}
