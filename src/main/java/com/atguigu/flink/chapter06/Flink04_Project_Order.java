package com.atguigu.flink.chapter06;

import com.atguigu.flink.bean.OrderEvent;
import com.atguigu.flink.bean.TxEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2022/8/3 9:00
 */
public class Flink04_Project_Order {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
    
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env
            .readTextFile("input/OrderLog.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new OrderEvent(
                    Long.valueOf(data[0]),
                    data[1],
                    data[2],
                    Long.valueOf(data[3])
                );
            })
            .filter(log -> "pay".equals(log.getEventType()));// 对账, 只需要支付记录
    
    
        SingleOutputStreamOperator<TxEvent> txEventStream = env
            .readTextFile("input/ReceiptLog.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new TxEvent(
                    data[0],
                    data[1],
                    Long.valueOf(data[2])
                );
            });
    
        orderEventStream
            .connect(txEventStream)
            .keyBy(OrderEvent::getTxId, TxEvent::getTxId)
            .process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
                Map<String, OrderEvent> orderEventMap = new HashMap<>();
                Map<String, TxEvent> txEventMap = new HashMap<>();
                
                // 处理第一个流的元素
                @Override
                public void processElement1(OrderEvent value,
                                            Context ctx,
                                            Collector<String> out) throws Exception {
                    // 支付记录先来, 去txEventMap 查询有没有对应的流记录, 如果有, 对账成功, 如果没有: 把自己存起来
                    TxEvent txEvent = txEventMap.get(ctx.getCurrentKey());
    
                    if (txEvent != null) {
                        out.collect("订单: " + value.getOrderId() + " 对账成功!!!!");
                    }else{
                        orderEventMap.put(ctx.getCurrentKey(), value);
                    }
    
                }
            
                // 处理第二个流的元素
                @Override
                public void processElement2(TxEvent value,
                                            Context ctx,
                                            Collector<String> out) throws Exception {
                    OrderEvent orderEvent = orderEventMap.get(ctx.getCurrentKey());
    
                    if (orderEvent != null) {
                        out.collect("订单: " + orderEvent.getOrderId() + " 对账成功!!!!");
                    }else{
                        txEventMap.put(ctx.getCurrentKey(), value);
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
