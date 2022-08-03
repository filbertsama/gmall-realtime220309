package com.atguigu.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
    
    public static void main(String[] args) {
    
    }
}