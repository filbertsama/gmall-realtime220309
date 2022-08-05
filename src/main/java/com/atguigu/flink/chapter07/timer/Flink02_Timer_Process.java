package com.atguigu.flink.chapter07.timer;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/8/5 15:11
 */
public class Flink02_Timer_Process {
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        // 当水位超过10, 5s后发出预警
        // 定时器是和key绑在一起, 必须先keyBy
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
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<WaterSensor>forMonotonousTimestamps()
                    .withTimestampAssigner((ws, ts) -> ws.getTs())
            )
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                
                private long ts;
                int lastVc = 0;
                
                // 参数1: 定时器触发时间
                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                    // 当定时器触发的时候, 执行这个方法
                    out.collect(ctx.getCurrentKey() + " 连续5s上升, 发出预警...");
                    
                    // 重新5s触发的定时器
                    lastVc = 0;
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 如果是第一个水位, 注册定时器:5s后触发
                    // 如果不是第一个水位: 判断是否大于上一次的水位, 如果上升, 则什么都不做
                    // 如果下降或相等, 删除定时器, 重新注册一个新的5s后触发的定时器
                    
                    if (lastVc == 0) {
                        // 第一个水位: 注册定时器
                        ts = value.getTs() + 5000;
                        System.out.println("注册定时器: " + ts);
                        ctx.timerService().registerEventTimeTimer(ts);
                    } else if (value.getVc() < lastVc) {
                        // 水位下降, 取消定时器
                        System.out.println("水位下降, 删除定时器: " + ts);
                        ctx.timerService().deleteEventTimeTimer(ts);
                        
                        ts = value.getTs() + 5000;
                        System.out.println("重新注册新的定时器: " + ts);
                        ctx.timerService().registerEventTimeTimer(ts);
                    } else {
                        System.out.println("水位上升, 什么都不做 ");
                    }
                    
                    
                    lastVc = value.getVc();
                    
                }
            })
            .print();
        
        
        env.execute();
    }
}
