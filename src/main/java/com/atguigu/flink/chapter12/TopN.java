package com.atguigu.flink.chapter12;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/8/13 10:22
 */
public class TopN {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        // 1. 建立一个动态表与文件关联
        tEnv.executeSql("create table ub(" +
                            " user_id bigint, " +
                            " item_id bigint, " +
                            " category_id bigint, " +
                            " behavior string, " +
                            " ts bigint, " +
                            " et as to_timestamp_ltz(ts, 0)," +
                            " watermark for et as et - interval '3' second " +
                            ")with(" +
                            "'connector' = 'filesystem', " +
                            "'path' = 'input/UserBehavior.csv', " +
                            "'format' = 'csv' " +
                            ")");
        
        
        // 2. 过滤出需要的数据: pv
        Table t1 = tEnv.sqlQuery("select item_id, et from ub where behavior='pv'");
        tEnv.createTemporaryView("t1", t1);
        
        // 3. 开窗聚合:统计每个商品在每个窗口内的点击量
        Table t2 = tEnv.sqlQuery("select " +
                                     " window_start, window_end, item_id, " +
                                     " count(*) ct " + // count(1) count(*) count(id) sum(1)
                                     "from table( tumble( table t1, descriptor(et), interval '1' hour) ) " +
                                     "group by window_start, window_end, item_id");
        
        tEnv.createTemporaryView("t2", t2);
        // 4. over窗口 row_number
        Table t3 = tEnv.sqlQuery("select " +
                                     " window_start, window_end, item_id, ct, " +
                                     " row_number() over(partition by window_end order by ct desc) rn " +
                                     "from t2");
        tEnv.createTemporaryView("t3", t3);
        
        // 5. 过滤 rn<=3
        Table result = tEnv.sqlQuery("select " +
                                        " * " +
                                        "from t3 " +
                                        "where rn <= 3");
    
        // 6. 把结果写出到支持更新的sink中  mysql
        tEnv.executeSql("CREATE TABLE `hot_item` (\n" +
                            "  `w_end` timestamp," +
                            "  `item_id` bigint," +
                            "  `item_count` bigint," +
                            "  `rk` bigint," +
                            "  PRIMARY KEY (`w_end`,`rk`) not enforced" +
                            ") with(" +
                            "    'connector' = 'jdbc',\n" +
                            "   'url' = 'jdbc:mysql://hadoop162:3306/flink_sql?useSSL=false',\n" +
                            "   'table-name' = 'hot_item', " +
                            "   'username' = 'root', " +
                            "   'password' = 'aaaaaa' " +
                            ")");
        
        tEnv.executeSql("insert into hot_item select window_end w_end, item_id, ct item_count, rn rk from " + result);
        
        
    }
}
