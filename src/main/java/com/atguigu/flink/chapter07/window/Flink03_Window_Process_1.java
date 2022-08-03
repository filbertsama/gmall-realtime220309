package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/8/3 14:18
 */
public class Flink03_Window_Process_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
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
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            // 统计平均水位
            .aggregate(
                new AggregateFunction<WaterSensor, Avg, Double>() {
                    
                    // 初始化一个累加器
                    // 每个窗口执行一次
                    @Override
                    public Avg createAccumulator() {
                        System.out.println("Flink03_Window_Process_1.createAccumulator");
                        return new Avg();
                    }
                    
                    // 累加方法
                    // 每来一个元素执行一次
                    @Override
                    public Avg add(WaterSensor value, Avg acc) {
                        System.out.println("Flink03_Window_Process_1.add");
                        acc.sum += value.getVc();
                        acc.count++;
                        return acc;
                    }
                    
                    // 返回最终的聚合结果
                    // 每个窗口执行一次
                    @Override
                    public Double getResult(Avg acc) {
                        System.out.println("Flink03_Window_Process_1.getResult");
                        return acc.sum * 1.0 / acc.count;
                    }
                    
                    // 合并累加器
                    // 注意: 这个方法只在session窗口会调用, 其他窗口不执行
                    @Override
                    public Avg merge(Avg a, Avg b) {
                        System.out.println("Flink03_Window_Process_1.merge");
                        return null;
                    }
                },
                new ProcessWindowFunction<Double, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<Double> elements,
                                        Collector<String> out) throws Exception {
                        System.out.println("Flink03_Window_Process.process");
                        Double result = elements.iterator().next();
    
                        String stt = AtguiguUtil.toDateTime(ctx.window().getStart());
                        String edt = AtguiguUtil.toDateTime(ctx.window().getEnd());
    
                        out.collect(key + " " + stt + " " + edt +  " " + result);
                    }
                }
            )
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
    public static class Avg {
        public Integer sum = 0;
        public Long count = 0L;
    }
}
/*
窗口内的元素进行处理, 用到窗口处理函数, 分两大类:

增量处理
    简单聚合
        sum, max, min
        maxBy minBy
    
    
    reduce
        输入和输出类型必须一致
    
    
    aggregate
        可以不一致, 中间有累加器做转换



全量处理
    
    process


 */