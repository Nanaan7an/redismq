package com.redismq.redismq.redis.dataredis.listener;

import cn.hutool.core.date.DateTime;
import com.redismq.redismq.redis.dataredis.common.util.RedisStreamUtils;
import com.redismq.redismq.redis.dataredis.config.SingleMqProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

/**
 * @Author Nanaan
 * @Date 2025/2/27 20:32
 * @Description 消费者监听器
 */
@Slf4j
@Component
public class ConsumerListener implements StreamListener<String, MapRecord<String, String, String>> {

    @Autowired
    SingleMqProperties singleMqProperties;

    @Autowired
    RedisStreamUtils redisStreamUtils;

    /**
     * 定义了接收到消息后的操作
     *
     * @param entries
     */
    @Override
    public void onMessage(MapRecord<String, String, String> entries) {
        log.info("***********监听到有新消息{}***********", DateTime.now());
        /**
         * 先获取一下消息的ID看看
         * 1.把应用run起来；
         * 2.直接在redis客户端窗口中新增一个消息；
         * 3.等待半天，发现并没有监听到任何消息；why？
         * StreamListener的实现类的作用类似于consumer；
         * 和在redis客户端中操作一样，在存在消息队列及消息的情况下，需要有消费者组及消费者存在，才可以实现消费消息；
         * 所以，应当先创建一个消费者组，并将ConsumerListener加入到这个消费者组中。
         */
        RecordId recordId = entries.getId();
        log.info("本次监听到的消息的ID是【{}】", recordId);

        //消费消息
        redisStreamUtils.readMag(
                singleMqProperties.getStreamName(),
                singleMqProperties.getGroupName(),
                singleMqProperties.getConsumerName());
    }
}
