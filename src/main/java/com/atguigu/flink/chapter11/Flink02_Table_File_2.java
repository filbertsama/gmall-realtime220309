package com.atguigu.flink.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lzc
 * @Date 2022/8/10 10:40
 */
public class Flink02_Table_File_2 {
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    
        Schema schema = new Schema()
            .field("id", DataTypes.STRING())
            .field("ts", DataTypes.BIGINT())
            .field("vc", DataTypes.INT());
        
        
        //建立到文件的连接
        // 数据会自动进入一个叫"sensor"的表中
    
        tEnv
            .connect(new FileSystem().path("input/sensor.txt"))
            .withFormat(new Csv()) // 行: \n  列: ,
            .withSchema(schema)
            .createTemporaryTable("sensor");
        
        
        // 得到一个table对象
    
        Table table = tEnv.from("sensor");
    
        Table result = table
            .where($("id").isEqual("sensor_1"))
            .groupBy($("id"))
            .select($("id"), $("vc").sum().as("vc_sum"));
        
        result.execute().print();
        
        // 创建一个动态表与文件管理
        Schema schema1 = new Schema()
            .field("id", DataTypes.STRING())
            .field("vc_sum", DataTypes.INT());
    
        tEnv
            .connect(new FileSystem().path("input/b.txt"))
            .withFormat(new Csv()) // 行: \n  列: ,
            .withSchema(schema1)
            .createTemporaryTable("abc");
        
        // 把result写入到文件中
        result.executeInsert("abc");
        
    
    
    }
}
