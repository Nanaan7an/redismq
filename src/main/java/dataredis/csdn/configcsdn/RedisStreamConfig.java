package dataredis.csdn.configcsdn;


import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import dataredis.csdn.RedisConsumersListener;
import dataredis.csdn.util.RedisStreamUtil;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import javax.annotation.Resource;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@Slf4j
public class RedisStreamConfig {

    @Resource
    private RedisStreamUtil redisStreamUtil;

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    @Autowired
    private RedisMqProperties redisMqProperties;

    @Autowired
    private RedisConsumersListener redisConsumersListener;

    /**
     * 多个订阅者对应同一个key时 会几乎同时获取到同一条消息
     * 如果两个消费组共用一个监听 消息就会在该监听器并发出现两次 即消息重复问题
     * 如果有N个消费组共用监听 就会有N的并发 这对去重要求很高
     * 推荐做法是 key-group 1对1  不同业务去使用不同的key就好了
     * 如果硬要 1对N 这对代码的封装性会有破坏，几乎就是把业务代码分别直接/间接写在不同的监听器里面了
     * 不同监听器知道哪些消息是属于它的 可以用简单的map映射 如下
     */
//    static Map<String, StreamListener> reflect = new HashMap<>();
//    static {
//        // xxxListener implements StreamListener
//        reflect.put("orderGroup", new OrderOrderListener());
//        reflect.put("goodsGroup", new OrderGoodsListener());
//        // ... put other
//    }

    /**
     * redis序列化
     *
     * @param redisConnectionFactory
     * @return {@code RedisTemplate<String, Object>}
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory);
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        om.activateDefaultTyping(om.getPolymorphicTypeValidator(), ObjectMapper.DefaultTyping.NON_FINAL);
        Jackson2JsonRedisSerializer jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer(om.getClass());
        StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();
        template.setKeySerializer(stringRedisSerializer);
        template.setHashKeySerializer(stringRedisSerializer);
        template.setValueSerializer(jackson2JsonRedisSerializer);
        template.setHashValueSerializer(jackson2JsonRedisSerializer);
        template.afterPropertiesSet();
        return template;
    }

    @ConditionalOnProperty(value = "redis.stream.enable", havingValue = "true", matchIfMissing = false)
    @Bean(initMethod = "start", destroyMethod = "stop")
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> subscriptions(RedisConnectionFactory factory) {

        List<RedisMqConfig> configs = Objects.requireNonNull(redisMqProperties.getConfigs(), "config error: config is null");

        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer
                        .StreamMessageListenerContainerOptions
                        .builder()
                        .executor(threadPoolExecutor)
                        // 每次从Redis Stream中读取消息的最大条数 (32为rocketmq的pullBatchSize默认数量)
                        .batchSize(32)
                        // 轮询拉取消息的时间 (如果流中没有消息，它会等待这么久的时间，然后再次检查。)
                        .pollTimeout(Duration.ofSeconds(1))
                        .errorHandler(throwable -> {
                            log.error("[redis MQ handler exception]", throwable);
                            throwable.printStackTrace();
                        })
                        .build();

        var listenerContainer = StreamMessageListenerContainer.create(factory, options);
        // 创建不同的订阅者
        List<Subscription> subscriptions = new ArrayList<>();
        for (int i = 0; i < configs.size(); i++) {
            subscriptions.add(createSubscription(listenerContainer, configs.get(i).getStreamName(), configs.get(i).getGroupName(), configs.get(i).getConsumerName()));
        }
        listenerContainer.start();
        return listenerContainer;
    }

    /**
     * @param listenerContainer
     * @param streamName        类似 topic
     * @param groupName         消费组是 Redis Streams 中的一个重要特性，它允许多个消费者协作消费同一个流中的消息。每个消费组可以有多个消费者。
     * @param consumerName      这是消费组中的具体消费者名称。每个消费者会从消费组中领取消息进行处理。
     * @return
     */
    private Subscription createSubscription(StreamMessageListenerContainer<String, MapRecord<String, String, String>> listenerContainer, String streamName, String groupName, String consumerName) {

        initStream(streamName, groupName);


        // 手动ask消息
//        Subscription subscription = listenerContainer.receive(Consumer.from(groupName, consumerName),
//                //  创建一个流的偏移量实例。 含义: 指定从哪个偏移量开始读取消息。ReadOffset.lastConsumed()表示从上次消费的位置开始。
//                StreamOffset.create(streamName, ReadOffset.lastConsumed()), redisConsumersListener);

        // 自动ask消息
//            Subscription subscription = listenerContainer.receiveAutoAck(Consumer.from(groupName, consumerName),
//                    StreamOffset.create(streamName, ReadOffset.lastConsumed()), redisConsumersListener);

        // 手动创建 核心在于 cancelOnError(t -> false)  出现异常不退出
        StreamMessageListenerContainer.ConsumerStreamReadRequest<String> build =
                StreamMessageListenerContainer.StreamReadRequest
                        .builder(StreamOffset.create(streamName, ReadOffset.lastConsumed()))
                        .consumer(Consumer.from(groupName, consumerName))
                        .autoAcknowledge(false)
                        // 重要！
                        .cancelOnError(t -> false).build();

        Subscription subscription = listenerContainer.register(build, redisConsumersListener);

        return subscription;
    }

    /**
     * 初始化流 保证stream流程是正常的
     *
     * @param key
     * @param group
     */
    private void initStream(String key, String group) {
        boolean hasKey = redisStreamUtil.hasKey(key);
        if (!hasKey) {
            Map<String, Object> map = new HashMap<>(1);
            map.put("field", "value");
            //创建主题
            String result = redisStreamUtil.addMap(key, map);
            //创建消费组
            redisStreamUtil.createGroupCsdn(key, group);
            //将初始化的值删除掉
            redisStreamUtil.del(key, result);
            log.info("stream:{}-group:{} initialize success", key, group);
        } else {
            List<String> existGroupList = new ArrayList<>();
            redisStreamUtil.queryGroups(key).forEach(
                    item -> existGroupList.add(item.groupName())
            );
            // 这里考虑的是同一个key需要不同消费组场景
            if (!existGroupList.contains(group)) {
                //创建消费组
                redisStreamUtil.createGroupCsdn(key, group);
            }
        }
    }


    /**
     * 可选方法： 校验 Redis 版本号，是否满足最低的版本号要求
     */
    private static void checkRedisVersion(RedisTemplate<String, ?> redisTemplate) {
        // 获得 Redis 版本
        Properties info = redisTemplate.execute((RedisCallback<Properties>) RedisServerCommands::info);
        String version = MapUtil.getStr(info, "redis_version");
        // 校验最低版本必须大于等于 5.0.0
        int majorVersion = Integer.parseInt(StrUtil.subBefore(version, '.', false));
        if (majorVersion < 5) {
            throw new IllegalStateException(StrUtil.format("您当前的 Redis 版本为 {}，小于最低要求的 5.0.0 版本！", version));
        }
    }
}


