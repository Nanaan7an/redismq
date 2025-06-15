package com.redismq.redismq.redis.dataredis.common.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Author Nanaan
 * Date 2025/2/27 20:20
 * Description 根据常用的redistribution命令，封装对应的方法
 * 该类主要封装RedisTemplate.opsForStream()相关的方法，用于操作Stream(Redis提供的一种用于消息处理的高级数据结构)
 */
@Component
public class RedisStreamUtils {
    /**
     * 补充一下术语，不同技术栈中有类似的作用：
     * Redis Stream = sofa MQ
     * 消息队列stream = group GID_XX
     * 消费者组group = topic
     * 消费者consumer = 监听者listener
     * （用sofa术语）一类相似的功能，通常创建一个group；根据topic去细分不同的业务处理方式；一般对于一个topic，也只创建一个监听者
     */

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;


    /**
     * (不存在消息队列时)创建消息队列，并向消息队列中写入消息
     * 功能与该命令一致》XADD streamKey * MapKey MapValue
     * 在执行该方法前、后分别执行，查看创建的信息》xrange streamKey - +
     *
     * @param streamKey 消息队列的键
     * @param msg       消息
     * @return 消息的ID
     */
    public String addMsg(String streamKey, Map<String, String> msg) {

        MapRecord<String, String, String> stream = StreamRecords.newRecord()
                .ofMap(msg)
                .withStreamKey(streamKey);

        return redisTemplate.opsForStream().add(stream).getValue();
    }

    /**
     * (不存在时)创建或(存在时)获取消息队列的消费者组
     * 在执行该方法前、后分别执行，查询消息队列的信息》XINFO GROUPS streamKey
     *
     * @param streamKey 消息队列的键
     * @param group     消费者组的名称
     */
    public void getGroup(String streamKey, String group) {
        //查询当前存在的消费者组
        StreamInfo.XInfoGroups xinfoGroups = redisTemplate.opsForStream().groups(streamKey);

        List<String> groups = new ArrayList<>();
        for (int i = 0; i < xinfoGroups.groupCount(); i++) {
            groups.add(xinfoGroups.get(i).groupName());
        }

        //不存在时则创建
        if (!groups.contains(group)) {
            redisTemplate.opsForStream().createGroup(streamKey, group);
        }
    }

    /**
     * 在消费者组group中，读取消息，且(不存在时)创建或(存在时)获取消费者
     * 类似于》XREADGROUP GROUP groupKey consumerKey COUNT 1 STREAMS streamKey >
     * 在执行该方法前、后，查询消费者组的信息》XINFO CONSUMERS streamKey groupKey
     *
     * @param streamKey 消息队列的键
     * @param group     消费者组的名称
     * @param consumer  消费者名称
     */
    public void readMag(String streamKey, String group, String consumer) {

        // (不存在消费者时)创建或(存在时)使用消费者，并读取消息
        List<MapRecord<String, Object, Object>> messages = redisTemplate.opsForStream().read(
                Consumer.from(group, consumer),
                StreamReadOptions.empty().count(1),//读取1条
                StreamOffset.create(streamKey, ReadOffset.lastConsumed())
        );
        System.out.println("messages==" + messages);
    }
}
