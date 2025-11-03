package dataredis.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
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
@Slf4j
public class RedisStreamUtils {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;


    /**
     * (不存在消息队列时创建消息队列，并)向消息队列中写入消息
     * 功能与该命令一致》XADD streamKey * MapKey MapValue
     * 在执行该方法前、后分别执行，查看创建的信息》XRANGE streamKey - +
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
     * 》XGROUP CREATE streamKey group 0
     * 在执行该方法前、后分别执行，查询消息队列的流以及消费者组的信息》XINFO GROUPS streamKey
     *
     * @param streamKey 消息队列的键
     * @param group     消费者组的名称
     */
    public void getGroup(String streamKey, String group) {
        StreamInfo.XInfoGroups xinfoGroups = null;
        try {
            //查询指定消息队列当前所有的消费者组
            xinfoGroups = redisTemplate.opsForStream().groups(streamKey);
        } catch (RedisSystemException e) {
            //当不存在任何消费者时则创建指定的消费者
            log.info("Redis Stream [{}] without any consumer,create group named [{}]", streamKey, group);
            redisTemplate.opsForStream().createGroup(streamKey, group);
        } finally {
            List<String> groups = new ArrayList<>();
            for (int i = 0; i < xinfoGroups.groupCount(); i++) {
                groups.add(xinfoGroups.get(i).groupName());
            }
            log.info("Redis Stream >>>{}", groups);
        }
    }

    /**
     * 读取消费者组group中的消息，且(不存在时)创建或(存在时)获取消费者
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
        System.out.println("messages=" + messages);
    }
}
