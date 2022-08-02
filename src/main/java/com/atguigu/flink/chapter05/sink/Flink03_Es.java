package com.atguigu.flink.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/2 13:55
 */
public class Flink03_Es {
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
        
        List<HttpHost> hosts = Arrays.asList(
            new HttpHost("hadoop162", 9200),
            new HttpHost("hadoop163", 9200),
            new HttpHost("hadoop164", 9200)
        );
        
        ElasticsearchSink.Builder<WaterSensor> builder = new ElasticsearchSink.Builder<WaterSensor>(
            hosts,
            new ElasticsearchSinkFunction<WaterSensor>() {
                @Override
                public void process(WaterSensor element,  // 需要写出的元素
                                    RuntimeContext ctx, // 运行时想下午
                                    RequestIndexer indexer) { // 把要写出的数据,封装到RequestIndexer
                    String msg = JSON.toJSONString(element);
    
                    IndexRequest ir = Requests
                        .indexRequest("sensor")
                        .type("_doc")  // 定义type的时候, 不能下划线开头. _doc是唯一的特殊情况
                        .id(element.getId())  // 定义每条数据的id. 如果不指定id, 会随机分配一个id. id重复的时候会更新数据
                        .source(msg, XContentType.JSON);
                    
                    indexer.add(ir);  // 把ir存入到indexer, 就会自动的写入到es中
                    
                }
            }
        );
        
        result.addSink(builder.build());
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
index  数据库
type    表(6.X开始type只能有一个, 6.x之前可以有多个. 从7.x开始 type被废弃)
document  行
field value 列



 */