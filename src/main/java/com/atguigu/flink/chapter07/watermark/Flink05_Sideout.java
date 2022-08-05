package com.atguigu.flink.chapter07.watermark;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/5 10:17
 */
public class Flink05_Sideout {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env.getConfig().setAutoWatermarkInterval(2000);
    
    
        SingleOutputStreamOperator<String> main = env
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
                    .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((ele, ts) -> ele.getTs())
                //.withIdleness(Duration.ofSeconds(5))  // 当某个并行度水印超过5s没有更新, 则以其他为准
            )
            .keyBy(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .sideOutputLateData(new OutputTag<WaterSensor>("late"){})
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                // 在允许迟到期间, 每来一个属于这个窗口的元素, 这个方法就会执行
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<WaterSensor> elements,
                                    Collector<String> out) throws Exception {
                    List<WaterSensor> list = AtguiguUtil.toList(elements);
                    String stt = AtguiguUtil.toDateTime(ctx.window().getStart());
                    String edt = AtguiguUtil.toDateTime(ctx.window().getEnd());
                
                    out.collect(key + " " + stt + " " + edt + " " + list);
                
                }
            });
    
        main.print("main");
        main.getSideOutput(new OutputTag<WaterSensor>("late"){}).print("late");
    
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
}
/*
flink如果保证数据不丢失:
1. 水印+事件时间
2. 允许迟到
3. 侧输出流
------------


 */