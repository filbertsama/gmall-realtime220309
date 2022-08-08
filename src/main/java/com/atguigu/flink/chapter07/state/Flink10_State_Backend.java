package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lzc
 * @Date 2022/8/5 15:11
 */
public class Flink10_State_Backend {
    public static void main(String[] args) throws Exception {
        // shift + ctrl + u
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        env.enableCheckpointing(2000);
        
        // 设置状态后端
        // 内存
        // 1. 旧
//        env.setStateBackend(new MemoryStateBackend());  // 默认状态: 本地在内存 + checkpoint在内存
        // 2. 新
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        
        // fs
        // 1. old
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/ck1"));
        
        //2. new
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck2");
        
        // rocksdcb
        //1. old
        //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop162:8020/ck3"));
        //2.new
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck4");
        
        env
            .socketTextStream("hadoop162", 9999) // socket只能是1
            .map(line -> {
                String[] data = line.split(",");
                return new WaterSensor(
                    data[0],
                    Long.valueOf(data[1]),
                    Integer.valueOf(data[2])
                );
            })
            .keyBy(WaterSensor::getId)
            .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                
                
                private MapState<Integer, Object> vcMapState;
                
                @Override
                public void open(Configuration parameters) throws Exception {
                    vcMapState = getRuntimeContext().getMapState(
                        new MapStateDescriptor<Integer, Object>(
                            "vcMapState",
                            TypeInformation.of(new TypeHint<Integer>() {}),
                            TypeInformation.of(new TypeHint<Object>() {})
                        )
                    
                    );
                    
                }
                
                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    
                    vcMapState.put(value.getVc(), new Object());
                    
                    
                    Iterable<Integer> vcs = vcMapState.keys();
                    
                    out.collect(ctx.getCurrentKey() + " 的所有不同水位: " + AtguiguUtil.toList(vcs));
                }
            })
            .print();
        
        
        env.execute();
    }
    
    
}
/*
设置状态后端和checkpoint, 有两个地方:
1. 配置文件中
    旧的写法:
         memory
            state.backend: jobmanager
          
         fs
            state.backend:filesystem
            state.checkpoints.dir: hdfs://namenode-host:port/flink-checkpoints
            
         rocksdb
           state.backend:filesystem: rocksdb
            state.checkpoints.dir: hdfs://namenode-host:port/flink-checkpoints

    新的写法:
        state.backend: hashmap  /rocksdb
        state.checkpoints.dir: 目录 / jobmanager
        
        本地rocksdb + jobmanager 这种组合一般不用
2. 在代码中
 */