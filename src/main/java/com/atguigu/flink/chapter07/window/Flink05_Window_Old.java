package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/3 14:18
 */
public class Flink05_Window_Old {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    
        env
            .socketTextStream("hadoop162", 9999)
            .map(line -> {
                String[] data = line.split(",");
            
                return new WaterSensor(
                    data[0],
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2])
                );
            })
            .keyBy(WaterSensor::getId)
            // 滚动窗口: 旧的写法
            .timeWindow(Time.seconds(5))
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            
                // 在窗口关闭的时候出触发一次
                @Override
                public void process(String key,
                                    Context ctx,  // 上下文对象: 里面封装了一些信息, 比如窗口开始时间结束, 定时器服务器...
                                    Iterable<WaterSensor> elements, // 存储了这个窗口内所有的元素
                                    Collector<String> out) throws Exception {
                
                    // 把Iterable中所有的元素取出存入到list集合中
                    List<WaterSensor> list = AtguiguUtil.toList(elements);
                
                    // 获取窗口相关信息:
                    String stt = AtguiguUtil.toDateTime(ctx.window().getStart());
                    String edt = AtguiguUtil.toDateTime(ctx.window().getEnd());
                
                
                    out.collect("窗口: " + stt + " " + edt + ", key:" + key + "  " + list);
                
                }
            })
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
