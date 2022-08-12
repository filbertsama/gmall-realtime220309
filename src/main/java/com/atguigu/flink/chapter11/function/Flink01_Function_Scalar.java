package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author lzc
 * @Date 2022/8/12 14:13
 */
public class Flink01_Function_Scalar {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        DataStreamSource<WaterSensor> stream = env.fromElements(
            new WaterSensor("sensor_1", 1000L, 10),
            new WaterSensor("sensor_1", 2000L, 20),
            new WaterSensor("sensor_2", 3000L, 30),
            new WaterSensor("sensor_1", 4000L, 40),
            new WaterSensor("sensor_1", 5000L, 50),
            new WaterSensor("sensor_1", 6000L, 60),
            new WaterSensor(null, 6000L, 100)
        );
        
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        
        Table table = tEnv.fromDataStream(stream);
        tEnv.createTemporaryView("sensor", table);
        
        
        // 1. 在table api中使用
        // 1.1 内联的方式使用
        /*table
            .select($("id"), call(MyUpper.class, $("id")).as("upper_id"))
            .execute()
            .print();*/
        // 1.2 先注册后使用
        tEnv.createTemporaryFunction("my_upper", MyUpper.class);
        /*table
            .select($("id"), call("my_upper", $("id")).as("upper_id"))
            .execute()
            .print();*/
        // 2. 在sql语句中使用
        // 一定要先注册临时表, 才能在sql中使用
        tEnv.sqlQuery("select " +
                          " id, " +
                          " my_upper(id, vc) id_upper " +
                          "from sensor")
            .execute()
            .print();
        
        
        
        
    }
    
    public static class MyUpper extends ScalarFunction {
        // 方法的名必须是eval
        // 参数和返回值根据世界情况来定
        public String eval(String s, Integer n) {
            if (s != null) {
                return s.toUpperCase() + n;
            }
            return null;
        }
        
    }
}
