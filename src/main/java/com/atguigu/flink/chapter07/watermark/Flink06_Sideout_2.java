package com.atguigu.flink.chapter07.watermark;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author lzc
 * @Date 2022/8/5 10:17
 */
public class Flink06_Sideout_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env.getConfig().setAutoWatermarkInterval(2000);
    
    
        SingleOutputStreamOperator<String> main = env
            .socketTextStream("hadoop162", 9999) // socket只能是1
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(
                    data[0],
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2])
                );
            })
            .process(new ProcessFunction<WaterSensor, String>() {
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    if ("sensor_1".equals(value.getId())) {
                        out.collect(value.toString());
                    }else if("sensor_2".equals(value.getId())){
                        ctx.output(new OutputTag<String>("s2"){}, value.toString() + "aaa");
                    }else{
                        ctx.output(new OutputTag<String>("other"){}, value.toString());
                    }
                }
            });
    
    
        main.print("main");
        DataStream<String> s2 = main.getSideOutput(new OutputTag<String>("s2") {});
        s2.print();
        main.getSideOutput(new OutputTag<String>("other"){}).print("other");
    
    
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
}
/*



 */