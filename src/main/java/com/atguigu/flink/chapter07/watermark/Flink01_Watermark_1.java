package com.atguigu.flink.chapter07.watermark;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/5 10:17
 */
public class Flink01_Watermark_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        
        env.getConfig().setAutoWatermarkInterval(2000);
        
        
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
            .assignTimestampsAndWatermarks(
                new WatermarkStrategy<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(
                        WatermarkGeneratorSupplier.Context context) {
                        return new MyWM();
                    }
                }
                    .withTimestampAssigner((ele, ts) -> ele.getTs())
            )
            .keyBy(WaterSensor::getId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                @Override
                public void process(String key,
                                    Context ctx,
                                    Iterable<WaterSensor> elements,
                                    Collector<String> out) throws Exception {
                    List<WaterSensor> list = AtguiguUtil.toList(elements);
                    String stt = AtguiguUtil.toDateTime(ctx.window().getStart());
                    String edt = AtguiguUtil.toDateTime(ctx.window().getEnd());
                    
                    out.collect(key + " " + stt + " " + edt + " " + list);
                    
                }
            })
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MyWM implements WatermarkGenerator<WaterSensor> {
        
        
        long maxTs = Long.MIN_VALUE + 3000 + 1;
        
        // 每来一条数据执行一次
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("MyWM.onEvent");
            
            maxTs = Math.max(maxTs, eventTimestamp);
            output.emitWatermark(new Watermark(maxTs - 3000));   // 打点式或间歇性
            
        }
        
        // 周期性的值: 200ms执行一次
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //            /System.out.println("MyWM.onPeriodicEmit");
//            output.emitWatermark(new Watermark(maxTs - 3000));   // 周期性的水印
        }
    }
    
}
