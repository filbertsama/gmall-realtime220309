package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/6 9:44
 */
public class Flink02_State_Operator_Union_List {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        
        // 开启checkpoint, 周期2000
        env.enableCheckpointing(2000);
        
        // 把这些单词存入到状态中, 当程序重启的时候, 可以把状态中的数据恢复
        env
            .socketTextStream("hadoop162", 9999)
            .map(new MyMapFunction())
            .print();
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static class MyMapFunction implements MapFunction<String, String>, CheckpointedFunction{
       
       List<String> words =  new ArrayList<>();
        private ListState<String> wordsState;
    
        @Override
        public String map(String line) throws Exception {
            if (line.contains("x")) {
                throw new RuntimeException("手动抛出异常");
            }
            
            String[] data = line.split(" ");
            words.addAll(Arrays.asList(data));
            return words.toString();
        }
    
        // 保存状态: 周期性的执行.
        // 每个并行度都会周期性的执行
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
//            System.out.println("MyMapFunction.snapshotState");
            // 把数据存入到算子状态(列表状态)
//            wordsState.clear(); // 清空状态
//            wordsState.addAll(words); // 向状态中写入数据
    
            wordsState.update(words);
            
        }
    
        // 程序启动的时候每个并行度执行一次.
        // 可以把状态中的数据恢复到java的集合中
        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            System.out.println("MyMapFunction.initializeState");
            // 从状态恢复数据  ctrl+alt+f
            // 联合列表: 每个并行度得到一个全量的状态
            wordsState = ctx.getOperatorStateStore().getUnionListState(new ListStateDescriptor<String>("wordsState", String.class));
            // 从列表状态中获取数据
            Iterable<String> it = wordsState.get();
            for (String word : it) {
                words.add(word);
            }
    
        }
    }
}
