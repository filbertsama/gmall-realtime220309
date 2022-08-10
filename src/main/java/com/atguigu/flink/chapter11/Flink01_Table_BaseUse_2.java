package com.atguigu.flink.chapter11;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/8/10 10:40
 */
public class Flink01_Table_BaseUse_2 {
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
//            .aggregate($("vc").sum().as("vc_sum"))
            .select($("id"), $("vc").sum().as("vc_sum"));
    
    
        SingleOutputStreamOperator<Row> resultStream = tEnv
            .toRetractStream(result, Row.class)
            .filter(t -> t.f0)
            .map(t -> t.f1);
    
        // 4. 输出结果
        resultStream.print();
        
        
        env.execute();
        
        
        
        
    
    
    }
}
