package com.atguigu.flink.chapter05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Arrays;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/2 13:55
 */
public class Flink03_Es_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        
        SingleOutputStreamOperator<WaterSensor> result = env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
            })
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
        // 自动刷新时间
        builder.setBulkFlushInterval(2000);  // 默认不会根据时间自动刷新.
        builder.setBulkFlushMaxSizeMb(1024); // 当批次中的数据大于等于这个值刷新
        builder.setBulkFlushMaxActions(2); // 每来的多少条数据刷新一次
        
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