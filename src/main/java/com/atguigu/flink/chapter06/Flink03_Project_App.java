package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Author lzc
 * @Date 2022/8/3 9:00
 */
public class Flink03_Project_App {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
    
        // 统计不同的渠道不同的行为的个数
        env
            .addSource(new AppSource())
            .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                    return Tuple2.of(value.getChannel() + "_" + value.getBehavior(), 1L);
                }
            })
            .keyBy(t -> t.f0)
            .sum(1)
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class AppSource implements SourceFunction<MarketingUserBehavior> {
    
        // 在这里把source读到的数据放入到流中
        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            Random random = new Random();
    
            String[] behaviors = {"download", "install", "update", "uninstall"};
            String[] channels = {"appstore", "huawei", "xiaomi", "vivo", "oppo"};
    
    
            while (true) {
                Long userId = (long)(random.nextInt(2000) + 1);  // [1, 2000]
                String behavior = behaviors[random.nextInt(behaviors.length)];
                String channel = channels[random.nextInt(channels.length)];
                Long timestamp = System.currentTimeMillis();
    
                ctx.collect(new MarketingUserBehavior(
                    userId,
                    behavior,
                    channel,
                    timestamp
                ));
    
                Thread.sleep(200);
                
            }
        }
    
        // 取消source读取数据
        // 这个方法不会自动执行, 可以在外面条用这个方法
        @Override
        public void cancel() {
        
        }
    }
    
}
