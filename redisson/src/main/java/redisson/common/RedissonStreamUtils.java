package com.example.demo.config.redis.redisson.common;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamGroup;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author Nanaan
 * @Date 2025/2/18 13:49
 * @Description 使用Redisson操作Redis Stream
 */
@Component
@Slf4j
public class RedissonStreamUtils {

    @Autowired
    RedissonClient redissonClient;

    /**
     * (存在时)获取或(不存在时)创建一个消息队列，并给这个消息队列Stream起个名字叫做key。
     * 调用此方法前、后，分别在服务器中查询所有键》keys *
     *
     * @param key
     * @return 返回值定义了这个队列的消息的格式/类型，如下（手动增加部分序号保持结构）：
     * 1) 1) "1739804684389-0"#服务器定义的ID
     * 1) 2) 1) "name"#RStream中的K，消息的属性名，一般使用String
     * 1) 2) 2) "zhangsan"#RStream中的V，消息的属性值，此处定义为Object
     * 这里的K-V规定了其他方法中消息的属性名和属性值的类型。
     * 简单来说，Redis中的Stream有三类参数：
     * 1.消息队列的键，在这个方法里是key；
     * 2.消息队列的消息，分为两种：
     * 2.1消息的属性名，类型一般是String；
     * 2.2消息的属性值，不限定类型；
     * 消息内的属性名和属性值可以存在多组；一个消息具有一个id。
     */
    public RStream<String, Object> createStream(String key) {
        return redissonClient.getStream(key);
    }

    /**
     * 向消息队列中添加消息
     * 调用此方法前、后分别查询队列中的消息》xrange key - +
     *
     * @param key  消息队列的键
     * @param data 消息队列的消息
     *             此处使用Map组织了消息的多组属性名K和属性值V，这里的K-V的类型需要与创建消息队列（RStream）时的类型分别匹配。
     * @return 服务器创建的消息ID
     */
    public String addMsg(String key, Map<String, Object> data) {

        //读取消息队列
        RStream<String, Object> rStream = createStream(key);

        //添加数据到消息队列
        StreamMessageId streamMessageId = rStream.add(StreamAddArgs.entries(data));

        //不指定消息ID、由服务器分配的ID由两部分组成，分别获取然后手动还原
        return streamMessageId.getId0() + "-" + streamMessageId.getId1();
    }

    /**
     * (存在时)什么都不做或(不存在时)创建一个消息队列
     * 调用此方法前、后分别查询消息队列的信息》XINFO GROUPS key
     *
     * @param key   消息队列的键
     * @param group 消息队列的消费者组的名字
     */
    public void createGroup(String key, String group) {

        //读取消息队列
        RStream<String, Object> rStream = createStream(key);

        //查询当前消费者
        List<String> groups = new ArrayList<>();
        for (StreamGroup item : rStream.listGroups()) {
            groups.add(item.getName());
        }
        log.info("消息队列【{}】当前拥有的消费者组：{}", key, groups);
        //创建消费者组
        if (!groups.contains(group)) {
            log.info("消息队列【{}】创建了新的消费者组：{}", key, group);
            rStream.createGroup(new StreamCreateGroupParams(group));
        }
    }
}
