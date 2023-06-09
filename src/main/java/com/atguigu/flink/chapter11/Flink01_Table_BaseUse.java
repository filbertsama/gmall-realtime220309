package com.atguigu.flink.chapter11;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author lzc
 * @Date 2022/8/10 10:40
 */
public class Flink01_Table_BaseUse {
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
        
        // 1. 把流转成表
        Table table = tEnv.fromDataStream(stream);
        table.printSchema();
        // 2. 在表上执行查询, 得到一个新的动态表
        // select * from t where id='sensor_1'
    
        Table result = table
            .where("id='sensor_1'")
//            .select("id, ts, vc");
            .select("id, vc");
    
        // 3. 把动态表转成流
//        DataStream<WaterSensor> resultStream = tEnv.toDataStream(result, WaterSensor.class);
//        DataStream<Row> resultStream = tEnv.toDataStream(result, Row.class);
        DataStream<Row> resultStream = tEnv.toAppendStream(result, Row.class);
    
        // 4. 输出结果
        resultStream.print();
        
        
        env.execute();
        
        
        
        
    
    
    }
}
