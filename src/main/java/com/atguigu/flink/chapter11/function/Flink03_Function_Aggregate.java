package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @Author lzc
 * @Date 2022/8/12 14:13
 */
public class Flink03_Function_Aggregate {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        DataStreamSource<WaterSensor> stream = env.fromElements(
            new WaterSensor("s1", 1000L, 10),
            new WaterSensor("s1", 2000L, 20),
            new WaterSensor("s2", 3000L, 30),
            new WaterSensor("s1", 4000L, 40),
            new WaterSensor("s1", 5000L, 50)
        );
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(stream);
        tEnv.createTemporaryView("sensor", table);
        
        
        // 1. 在table api中使用
        // 1.1 内联的方式使用
        // 1.2 先注册后使用
        tEnv.createFunction("my_avg", MyAvg.class);
        /*table
            .groupBy($("id"))
            .aggregate(call("my_avg", $("vc")).as("vc_avg"))
            .select($("id"), $("vc_avg"))
            .execute()
            .print();*/
        
       
        // 2. 在sql语句中使用
        // 一定要先注册临时表, 才能在sql中使用
        tEnv.sqlQuery("select " +
                          "id , my_avg(vc) vc_avg " +
                          "from sensor " +
                          "group by id")
            .execute()
            .print();
      
    }
    
    public static class MyAvg extends AggregateFunction<Double, SumCount> {
    
        // 初始化一个累加器
        @Override
        public SumCount createAccumulator() {
            return new SumCount();
        }
        
        // 真正实现累加的方法
        // 返回值: 必须是void
        // 参数1: 必须是累加器
        // 参数2..: 根据你实际情况来定
        public void accumulate(SumCount acc, Integer vc){
            acc.sum +=vc;
            acc.count++;
        }
        
        
    
        // 返回最终的聚合结果
        @Override
        public Double getValue(SumCount acc) {
            return acc.sum * 1.0 / acc.count;
        }
    
       
    }
    
    
    public static class SumCount{
        public Integer sum = 0;
        public Long count = 0L;
    }
   
    
    
}
/*



 */