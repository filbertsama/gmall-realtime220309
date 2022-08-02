package com.atguigu.flink.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @Author lzc
 * @Date 2022/8/2 11:02
 */
public class Flink01_Sink_Kafka_1 {
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
    
        Properties sinkConfig = new Properties();
        sinkConfig.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        stream
            .keyBy(WaterSensor::getId)
            .sum("vc")
            .addSink(new FlinkKafkaProducer<WaterSensor>(
                "default",  // 默认topic, 这个一般用不上
                new KafkaSerializationSchema<WaterSensor>() {  // 自定义的序列化器
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(WaterSensor element,
                                                                    @Nullable Long timestamp) {
                        String s = JSON.toJSONString(element);
                        return new ProducerRecord<>("s1", s.getBytes(StandardCharsets.UTF_8));
                    }
                },
                sinkConfig,  // kafka的配置
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE  // 一致性语义: 现在只能传入至少一次
            ));
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
