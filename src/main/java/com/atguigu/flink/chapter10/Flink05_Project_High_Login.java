package com.atguigu.flink.chapter10;

import com.atguigu.flink.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/8/8 14:05
 */
public class Flink05_Project_High_Login {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        KeyedStream<LoginEvent, Long> stream = env
//            .readTextFile("input/LoginLog.csv")
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new LoginEvent(
                    Long.valueOf(data[0]),
                    data[1],
                    data[2],
                    Long.parseLong(data[3]) * 1000
                );
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((log, ts) -> log.getEventTime())
            )
            .keyBy(LoginEvent::getUserId);
        
        // 1. 定义模式
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
            .<LoginEvent>begin("fail")
            .where(new SimpleCondition<LoginEvent>() {
                @Override
                public boolean filter(LoginEvent value) throws Exception {
                    return "fail".equals(value.getEventType());
                }
            })
            .times(2)
            .consecutive()
            .within(Time.milliseconds(2001));
        
        // 2. 将模式作用到流上
        PatternStream<LoginEvent> ps = CEP.pattern(stream, pattern);
        
        // 3. 从模式流取出匹配到的数据
        ps
            .select(new PatternSelectFunction<LoginEvent, String>() {
                @Override
                public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                    return pattern.get("fail").get(0).getUserId() +" 正在恶意登录...";
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
