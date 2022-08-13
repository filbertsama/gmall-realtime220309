package com.atguigu.flink.chapter13;

import com.atguigu.flink.bean.UserBehavior;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/8/8 14:05
 */
public class Flink02_Project_High_UV {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env
            .readTextFile("input/UserBehavior.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new UserBehavior(
                    Long.valueOf(data[0]),
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2]),
                    data[3],
                    Long.parseLong(data[4]) * 1000
                );
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((ub, ts) -> ub.getTimestamp())
            )
            .filter(ub -> "pv".equals(ub.getBehavior()))
            .windowAll(SlidingEventTimeWindows.of(Time.hours(2), Time.hours(2)))
            .process(new ProcessAllWindowFunction<UserBehavior, String, TimeWindow>() {
    
               
    
                @Override
                public void open(Configuration parameters) throws Exception{
                
                }
                
                @Override
                public void process(Context ctx,
                                    Iterable<UserBehavior> elements,
                                    Collector<String> out) throws Exception {
                    BloomFilter<Long> bf = BloomFilter.create(Funnels.longFunnel(), 100 * 10, 0.01);
    
                    int count = 0;
                    for (UserBehavior element : elements) {
                        if (bf.put(element.getUserId())) {
                            count++;
                        }
                    }
                    
                    
                    String stt = AtguiguUtil.toDateTime(ctx.window().getStart());
                    String edt = AtguiguUtil.toDateTime(ctx.window().getEnd());
    
                    out.collect(stt + " " + edt + " " + count);
                    
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
