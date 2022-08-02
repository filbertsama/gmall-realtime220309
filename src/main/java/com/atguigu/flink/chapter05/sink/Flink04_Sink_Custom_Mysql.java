package com.atguigu.flink.chapter05.sink;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * @Author lzc
 * @Date 2022/8/2 13:55
 */
public class Flink04_Sink_Custom_Mysql {
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
        
        result.addSink(new MySqlSink());
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    public static class MySqlSink extends RichSinkFunction<WaterSensor> {
        
        private Connection connection;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            // 建立到Mysql的连接
            // 1. 加载驱动
            Class.forName("com.mysql.jdbc.Driver");
            // 2. 通过驱动管理器获取连接对象  ctrl + alt + f 提升成员变量
            connection = DriverManager.getConnection("jdbc:mysql://hadoop162:3306/test?useSSL=false", "root", "aaaaaa");
        }
        
        @Override
        public void close() throws Exception {
            if (connection != null) {
                connection.close();
            }
        }
        
        // 调用: 每来一条元素,这个方法执行一次
        @Override
        public void invoke(WaterSensor value,
                           Context context) throws Exception {
            // jdbc的方式向mysql写数据
//            String sql = "insert into sensor(id, ts, vc)values(?,?,?)";
            // 如果主键不重复就新增, 主键重复, 就更新
//            String sql = "insert into sensor(id, ts, vc)values(?,?,?) on duplicate key update vc=?";
            String sql = "replace into sensor(id, ts, vc)values(?,?,?)";
            // 1. 得到预处理语句
            PreparedStatement ps = connection.prepareStatement(sql);
            // 2. 给sql中的占位符进行赋值
            ps.setString(1, value.getId());  // 占位符的索引是从1开始
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());
//            ps.setInt(4, value.getVc());
            // 3. 执行
            ps.execute();
            // 4. 提交
//            connection.commit(); // mysql默认自动提交, 所以这个地方不用调用
            // 5. 关闭预处理
            ps.close();
            
        }
    }
}
/*
 
 
 
 */