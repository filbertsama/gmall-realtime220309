package com.atguigu.flink.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.ArrayList;

/**
 * @Author lzc
 * @Date 2022/8/2 11:24
 */
public class Flink02_Redis_3 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        
        
        DataStreamSource<WaterSensor> stream = env.fromCollection(waterSensors);
        
        SingleOutputStreamOperator<WaterSensor> result = stream
            .keyBy(WaterSensor::getId)
            .sum("vc");
        
        
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
            .setHost("hadoop162")
            .setPort(6379)
            .setMaxTotal(100)
            .setMaxIdle(10)
            .setMinIdle(2)
            .setTimeout(10 * 1000)
            .build();
        // 写出到redis中
        result
            .addSink(new RedisSink<>(config, new RedisMapper<WaterSensor>() {
                
                // 返回命令描述符:
                @Override
                public RedisCommandDescription getCommandDescription() {
                    
                    return new RedisCommandDescription(RedisCommand.HSET, "s"); // hash的外层的key
                }
                
                @Override
                public String getKeyFromData(WaterSensor data) {
                    return data.getId();  // 对hash来说, 是内部的key: field
                }
                
                @Override
                public String getValueFromData(WaterSensor data) {
                    return JSON.toJSONString(data);
                }
                
            }));
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
string

key         value
sensor_1    json格式字符串

-------

list

sensor_1   [json格式字符串, ....]

set

sensor_1   [json格式字符串, ....]


hash

key    field      value

"s"    "sensor_1"  JSON格式字符串

 */