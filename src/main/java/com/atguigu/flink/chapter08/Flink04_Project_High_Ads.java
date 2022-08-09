package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.AdsClickLog;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author lzc
 * @Date 2022/8/8 14:05
 */
public class Flink04_Project_High_Ads {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        SingleOutputStreamOperator<String> main = env
            .readTextFile("input/AdClickLog.csv")
            .map(line -> {
                String[] data = line.split(",");
                return new AdsClickLog(
                    Long.valueOf(data[0]),
                    Long.valueOf(data[1]),
                    data[2],
                    data[3],
                    Long.parseLong(data[4]) * 1000
                );
            })
            .keyBy(log -> log.getUserId() + "_" + log.getAdsId())
            .process(new KeyedProcessFunction<String, AdsClickLog, String>() {
    
                private ValueState<String> yesterdayState;
                private ValueState<Boolean> isAddedBlackListState;
                private ReducingState<Long> clickCountState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    clickCountState = getRuntimeContext().getReducingState(
                        new ReducingStateDescriptor<Long>("clickCountState", Long::sum, Long.class));
                    
                    isAddedBlackListState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isAddedBlackListState", Boolean.class));
                    yesterdayState = getRuntimeContext().getState(new ValueStateDescriptor<String>("yesterdayState", String.class));
                }
                
                @Override
                public void processElement(AdsClickLog log,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    // 如何判断跨天
                    String today = AtguiguUtil.toDate(log.getTimestamp());
    
                    // 今天和状态中的日期不等
                    if (!today.equals(yesterdayState.value())) {
                        yesterdayState.update(today);
                        
                        // 清空其他两个状态
                        clickCountState.clear();
                        isAddedBlackListState.clear();
                    }
                    
                    
                    
                    if (isAddedBlackListState.value() == null) {
                        // 如果没有加入黑名单才需要进行累加
                        clickCountState.add(1L);
                    }
                    
                    
                    Long count = clickCountState.get();
                    
                    String msg = "用户:" + log.getUserId() + " 对广告:" + log.getAdsId() + " 的点击是: " + count;
                    if (count >= 100) {
                        if (isAddedBlackListState.value() == null) {
                            ctx.output(new OutputTag<String>("blackList") {}, msg + " 超过阈值100, 加入黑名单");
                            
                            isAddedBlackListState.update(true);
                            
                        }
                    } else {
                        out.collect(msg);
                    }
                }
            });
        
        main.print("正常");
        main.getSideOutput(new OutputTag<String>("blackList") {}).print("黑名单");
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
