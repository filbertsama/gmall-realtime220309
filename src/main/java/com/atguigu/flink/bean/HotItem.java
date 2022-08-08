package com.atguigu.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lzc
 * @Date 2022/8/8 15:17
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HotItem {
    
    private Long itemId;
    private Long wEnd;
    private Long count;
}
