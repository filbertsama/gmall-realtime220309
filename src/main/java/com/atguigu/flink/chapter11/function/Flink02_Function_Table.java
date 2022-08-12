package com.atguigu.flink.chapter11.function;

import com.atguigu.flink.bean.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lzc
 * @Date 2022/8/12 14:13
 */
public class Flink02_Function_Table {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        DataStreamSource<WaterSensor> stream = env.fromElements(
            new WaterSensor("ab a c", 1000L, 10),
            new WaterSensor("aa aaaa aaaa", 2000L, 20),
            new WaterSensor("atguigu aaaa", 3000L, 30),
            new WaterSensor("aaa cc  ddddd", 4000L, 40),
            new WaterSensor("hello world hello", 5000L, 50)
        );
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        Table table = tEnv.fromDataStream(stream);
        tEnv.createTemporaryView("sensor", table);
        
        
        // 1. 在table api中使用
        // 1.1 内联的方式使用
        // 1.2 先注册后使用
        tEnv.createFunction("my_split", MySplit.class);
        
        table
            .joinLateral(call("my_split", $("id")))  // 把传入的字符串id的值, 给做成了表
            .select($("id"), $("word"), $("len"))
            .execute()
            .print();
        // 2. 在sql语句中使用
        // 一定要先注册临时表, 才能在sql中使用
        
        
    }
    
    /*@FunctionHint(output = @DataTypeHint("row<word string, len int>"))
    public static class MySplit extends TableFunction<Row> {
        public void eval(String s) {
            String[] words = s.split(" ");
            
            for (String word : words) {
                collect(Row.of(word, word.length()));  // Row.of(a,b,c) 表示3列
            }
        }
    }*/
    
    public static class MySplit extends TableFunction<WordLen> {
        public void eval(String s) {
            String[] words = s.split(" +");
            
            for (String word : words) {
                collect(new WordLen(word, word.length()));  // Row.of(a,b,c) 表示3列
            }
        }
    }
    
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WordLen{
        private String word;
        private Integer len;
    }
}
/*
"ab a c"
            ab      2
            a       1
            c       1
            
"atguigu aaa"
            atguigu  7
            aaa      3


 */