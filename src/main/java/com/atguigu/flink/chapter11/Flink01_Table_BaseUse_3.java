package com.atguigu.flink.chapter11;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/8/10 10:40
 */
public class Flink01_Table_BaseUse_3 {
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        DataStreamSource<WaterSensor> stream = env.fromElements(
            new WaterSensor("sensor_1", 1000L, 10),
            new WaterSensor("sensor_1", 2000L, 20),
            new WaterSensor("sensor_2", 3000L, 30),
            new WaterSensor("sensor_1", 4000L, 40),
            new WaterSensor("sensor_1", 5000L, 50),
            new WaterSensor("sensor_1", 6000L, 60)
        );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(stream);
        
        // select id, sum(vc) as vc_sum from t group by id
        
        
        Table result = table
            .groupBy($("id"))
            .select($("id"), $("vc").sum().as("vc_sum"));
        
        
        result.execute().print();  // 阻塞式方法. 一般只在idea端或者sql客户端测试  在yarn上不用. print 连接器可以用在yarn测试
        
        
    }
}
