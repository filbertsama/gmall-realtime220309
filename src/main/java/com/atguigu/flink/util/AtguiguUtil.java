package com.atguigu.flink.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/8/3 14:28
 */
public class AtguiguUtil {
    public static <T> List<T> toList(Iterable<T> it) {
        List<T> list = new ArrayList<>();
        for (T t : it) {
            list.add(t);
            
        }
        return list;
    }
    
    public static String toDateTime(long ts) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
    }
}
