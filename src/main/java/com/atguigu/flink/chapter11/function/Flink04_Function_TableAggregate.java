package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lzc
 * @Date 2022/8/12 14:13
 */
public class Flink04_Function_TableAggregate {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        DataStreamSource<WaterSensor> stream = env.fromElements(
            new WaterSensor("s1", 1000L, 10),
            new WaterSensor("s1", 2000L, 20),
            new WaterSensor("s2", 3000L, 30),
            new WaterSensor("s1", 4000L, 15),
            new WaterSensor("s1", 5000L, 50)
        );
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(stream);
        tEnv.createTemporaryView("sensor", table);
        
        
        // 1. 在table api中使用
        // 1.1 内联的方式使用
        // 1.2 先注册后使用
        tEnv.createFunction("top2", Top2.class);
        
        table
            .groupBy($("id"))
            .flatAggregate(call("top2", $("vc")))
            .select($("id"), $("rank"), $("vc"))
            .execute()
            .print();
        
        // 2. 在sql语句中使用
        // 不支持
        
    }
    
    public static class Top2 extends TableAggregateFunction<Result, FirstSecond> {
        
        // 初始化累加器
        @Override
        public FirstSecond createAccumulator() {
            return new FirstSecond();
        }
        
        // 累加功能
        public void accumulate(FirstSecond fs, Integer vc) {
            if (vc > fs.first) {
                fs.second = fs.first;
                fs.first = vc;
            } else if (vc > fs.second) {
                fs.second = vc;
            }
        }
        
        // 发射数据
        public void emitValue(FirstSecond fs, Collector<Result> out) {
            out.collect(new Result("第一名", fs.first));
            if (fs.second > Integer.MIN_VALUE) {
                out.collect(new Result("第二名", fs.second));
            }
        }
        
    }
    
    
    public static class Result {
        public String rank;
        public Integer vc;
        
        public Result(String rank, Integer vc) {
            this.rank = rank;
            this.vc = vc;
        }
    }
    
    public static class FirstSecond {
        public Integer first = Integer.MIN_VALUE;
        public Integer second = Integer.MIN_VALUE;
    }
    
    
}
/*
每来一个水位, 输出第一名和第二名

new WaterSensor("s1", 1000L, 10)
    第一名   10
new WaterSensor("s1", 2000L, 20),
    第一名   20
    第二名   10
new WaterSensor("s2", 3000L, 30),
    第一名   30
    第二名   20
    
    ......


 */