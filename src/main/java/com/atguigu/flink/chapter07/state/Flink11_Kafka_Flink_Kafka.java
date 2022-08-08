package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic.EXACTLY_ONCE;

/**
 * @Author lzc
 * @Date 2022/8/8 10:22
 */
public class Flink11_Kafka_Flink_Kafka {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        // 开启checkpoint
        env.enableCheckpointing(2000);
        env.setStateBackend(new HashMapStateBackend());
        // 设置checkpoint 目录
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck100");
        
        // 设置checkpoint的维 严格一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        
        // 限制同时进行的checkpoint的数量的上限
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        
        // 两个checkpoint之间最小的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 1.13.6 新增的方法: 当程序取消的时候保留hdfs中的ck数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        
        
        
        Properties sourceProps = new Properties();
        sourceProps.put("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        sourceProps.put("group.id", "Flink11_Kafka_Flink_Kafka");
        
        Properties sinkProps = new Properties();
        sinkProps.put("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        sinkProps.put("transaction.timeout.ms", 15 * 60 * 1000);
        
        env
            .addSource(
                new FlinkKafkaConsumer<String>("s1", new SimpleStringSchema(), sourceProps)
                    .setStartFromLatest() // 当启动的时候, 如果没有消费记录, 从这个地方开始消费.
            )
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                    for (String word : value.split(" ")) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }
            })
            .keyBy(t -> t.f0)
            .sum(1)
            .addSink(new FlinkKafkaProducer<Tuple2<String, Long>>(
                "default", // 没用
                new KafkaSerializationSchema<Tuple2<String, Long>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> element,
                                                                    @Nullable Long timestamp) {
                        return new ProducerRecord<>("s2", (element.f0 + "_" + element.f1).getBytes(StandardCharsets.UTF_8));
                    }
                },
                sinkProps,
                EXACTLY_ONCE
            ));
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
