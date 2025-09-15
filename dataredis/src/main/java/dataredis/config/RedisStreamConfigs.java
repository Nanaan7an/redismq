package dataredis.config;

import dataredis.common.util.RedisStreamUtils;
import dataredis.listener.ConsumerListener;
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
 * @Date 2025/2/27 21:01
 * @Description 配置消费者组中的消费者；在启动时就会在容器中装配，一旦有新消息到达，就会调用Listener中的onMessage方法
 */
@Configuration
public class RedisStreamConfigs {
    //刚才实现的监听器
    @Autowired
    ConsumerListener listener;

    //用于创建Redis连接
    @Autowired
    RedisConnectionFactory redisConnectionFactory;

    //获取配置信息
    @Autowired
    SingleMqProperties singleMqProperties;

    @Autowired
    RedisStreamUtils redisStreamUtils;

    /**
     * Redis Stream中有新消息，StreamMessageListenerContainer负责获取消息，并将消息传递给Listener处理
     * @return
     */
    @Bean
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMsgListenerContainer() {

        //StreamMessageListenerContainerOptions：用于配置监听容器的选项，定义监听器的行为
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
                        .pollTimeout(Duration.ofSeconds(1))//必填，监听容器轮询的时间间隔，当未读取到消息时，每隔这个时间去读取一下Stream
                        .build();

        //创建监听容器
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> listenerContainer =
                StreamMessageListenerContainer.create(redisConnectionFactory, options);

        //已经存在redis中的key
//        String streamKey = "mqs";
        String streamKey = singleMqProperties.getStreamName();

        //在这个kay中已经创建的消费者组
//        String groupName = "sg1";
        String groupName = singleMqProperties.getGroupName();

        //从消息队列指定位置（此处是开始位置）开始读取，并将消息传递给监听者；从头开始读取可能导致重复消费未ACK的消息
        listenerContainer.receive(StreamOffset.fromStart(streamKey), listener);
        //顺带一提，从末尾位置开始读取是这样写
//        StreamOffset.latest(streamKey, groupName);

        //创建消费者组
        redisStreamUtils.getGroup(streamKey, groupName);


        //启动监听器，开始监听Stream的并处理接收到的消息
        listenerContainer.start();

        return listenerContainer;
    }
}
