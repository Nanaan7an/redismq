package com.redismq.redismq.redis.dataredis.beagle.config;

import lombok.Data;

/**
 * @Author Nanaan
 * @Date 2025/6/15 23:20
 * @Description 封装一些需要放到消息队列中的属性
 */

@Data
public class Beagle {
    //创造出来的小比都有自己的名字
    private String name;
}
