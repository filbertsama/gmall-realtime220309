package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/8/6 9:44
 */
public class Flink03_State_Operator_BroadCast {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        // 开启checkpoint, 周期2000
        env.enableCheckpointing(2000);
        
        
        // 获取一个数据流
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop162", 8888);
        
        // 获取一个配置流
        DataStreamSource<String> configStream = env.socketTextStream("hadoop162", 9999);
        
        MapStateDescriptor<String, String> bcStateDesc = new MapStateDescriptor<>("bcState", String.class, String.class);
        // 1. 把配置流做成广播流
        BroadcastStream<String> bcStream = configStream.broadcast(bcStateDesc);
        
        // 2. 让数据流去connect广播流
        BroadcastConnectedStream<String, String> coStream = dataStream.connect(bcStream);
        
        
        coStream
            .process(new BroadcastProcessFunction<String, String, String>() {
    
                // 4. 处理数据流中的数据: 从广播状态中取配置
                @Override
                public void processElement(String value,
                                           ReadOnlyContext ctx,
                                           Collector<String> out) throws Exception {
                    System.out.println("Flink03_State_Operator_BroadCast.processElement");
                    ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(bcStateDesc);
    
                    String conf = state.get("aSwitch");
    
                    if ("1".equals(conf)) {
                        out.collect(value + " 使用 1 号逻辑...");
                    } else if ("2".equals(conf)) {
                        out.collect(value + " 使用 2 号逻辑...");
                    }else if ("3".equals(conf)) {
                        out.collect(value + " 使用 3 号逻辑...");
                    }else{
                        out.collect(value + " 使用 default 号逻辑...");
                    }
                }
    
               
                // 3. 把广播流中的数据放入到广播状态
                @Override
                public void processBroadcastElement(String value,  // 配置数据
                                                    Context ctx,  // 上下文
                                                    Collector<String> out) throws Exception {
                    System.out.println("Flink03_State_Operator_BroadCast.processBroadcastElement");
                    
                    // 获取广播状态, 把配置信息写入到状态中
                    BroadcastState<String, String> state = ctx.getBroadcastState(bcStateDesc);
                    state.put("aSwitch", value);
                    
    
                }
            })
            .print();
        
        
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
}
