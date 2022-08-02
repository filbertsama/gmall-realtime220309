package com.atguigu.flink.chapter05.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/2 9:06
 */
public class Flink01_Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 如果不设置并行度, 则使用的默认并行度: 在idea是和cpu的核心数一致
        env.setParallelism(2);
    
        DataStreamSource<Integer> stream = env.fromElements(10, 20, 11, 15, 22, 30);
//        stream.print("shuffle前");
//        DataStream<Integer> result = stream.shuffle();
//        DataStream<Integer> result = stream.rebalance();  // 轮询的时候, 可能会跨 taskManager
        DataStream<Integer> result = stream.rescale();  // 效率更高. 不会跨节点
        result.print("shuffle后");
    
    
        env.execute();
    }
}
/*
对流重新分区的几个算子:
1. keyBy
     按照key进行分区.    对key进行双重hash然后进行分区

2. shuffle
    随机分区

3. rebalance
    平均分布. 数据重新分区的时候, 可能会跨taskManager
    
4. rescale
     平均分布. 效率比较rebalance高, 因为他是不会跨TaskManager
 */