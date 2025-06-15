package com.redismq.redismq.redis.dataredis.beagle.config;

import com.redismq.redismq.redis.dataredis.beagle.consumer.BeagleConsumer;
import com.redismq.redismq.redis.dataredis.common.util.RedisStreamUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.time.Duration;

/**
 * @Author Nanaan
 * @Description 配置消费者组中的消费者；在启动时就会在容器中装配，一旦有新消息到达，就会调用Listener中的onMessage方法
 */
@Configuration
public class BeagleStreamConfigs {

    //消费者
    @Autowired
    BeagleConsumer consumer;

    @Autowired
    RedisConnectionFactory redisConnectionFactory;

    @Autowired
    BeaglProperties properties;

    @Autowired
    RedisStreamUtils redisStreamUtils;

    /**
     * Redis Stream中有新消息/创造出的小比，StreamMessageListenerContainer负责获取消息/小比，并将消息传递给consumer处理/wer
     *
     * @return
     */
    @Bean
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMsgListenerContainer() {

        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
                        .pollTimeout(Duration.ofSeconds(1))
                        .build();

        StreamMessageListenerContainer<String, MapRecord<String, String, String>> listenerContainer =
                StreamMessageListenerContainer.create(redisConnectionFactory, options);

        String stream = properties.getStream();

        listenerContainer.receive(StreamOffset.fromStart(stream), consumer);

        String group = properties.getGroup();

        redisStreamUtils.getGroup(stream, group);

        listenerContainer.start();

        return listenerContainer;
    }
}
