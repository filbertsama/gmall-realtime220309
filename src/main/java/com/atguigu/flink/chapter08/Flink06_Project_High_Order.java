package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.OrderEvent;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/8 14:05
 */
public class Flink06_Project_High_Order {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        env
            .readTextFile("input/OrderLog.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new OrderEvent(
                    Long.valueOf(data[0]),
                    data[1],
                    data[2],
                    Long.parseLong(data[3]) * 1000
                );
            })
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((log, ts) -> log.getEventTime())
            )
            .keyBy(OrderEvent::getOrderId)
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            .process(new ProcessWindowFunction<OrderEvent, String, Long, TimeWindow>() {
    
                private ValueState<OrderEvent> createState;
    
                @Override
                public void open(Configuration parameters) throws Exception {
                    createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", OrderEvent.class));
                }
    
                @Override
                public void process(Long orderId,
                                    Context context,
                                    Iterable<OrderEvent> elements,
                                    Collector<String> out) throws Exception {
                    List<OrderEvent> list = AtguiguUtil.toList(elements);
                    if (list.size() == 2) {
                        System.out.println("订单: " + orderId + " 正常创建和支付...");
                    }else{
                        // 这个窗口内只有一个记录
                        // 如果是create放入到状态
                        
                        // 如果是pay, 判断create状态中是否有值: 如果有值, 表示超时支付.
                        // 没有create, 表示只有pay没有create
                        OrderEvent event = list.get(0);
                        if ("create".equals(event.getEventType())) {
                            createState.update(event);
                        }else{
                            if (createState.value() == null) {
                                // pay来的时候, 没有create
                                out.collect("订单:" + orderId + " 只有pay没有create, 请检查系统是否有bug...");
                            }else{
                                out.collect("订单:" + orderId + " 超时支付, 请检查系统是否有bug...");
                            }
                        }
                    }
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
