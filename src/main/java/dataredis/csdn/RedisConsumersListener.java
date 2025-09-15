package dataredis.csdn;
import dataredis.csdn.util.RedisStreamUtil;
import dataredis.csdn.consumercsdn.RedisConsumer;
import dataredis.csdn.consumercsdn.RedisMsg;
import dataredis.csdn.consumercsdn.RedisStream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@Component
@Slf4j
public class RedisConsumersListener implements StreamListener<String, MapRecord<String, String, String>> {

    @Autowired
    private Map<String, RedisConsumer> redisConsumer;
    @Autowired
    private RedisStreamUtil redisStreamUtil;

    /**
     * 监听器
     *
     * @param message
     */
    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        System.out.println("监听测试:" + message.getId());
        // stream的key值
        String streamName = message.getStream();
        //消息ID
        RecordId recordId = message.getId();
        //消息内容
        Map<String, String> msg = message.getValue();

        Map<String,Map<String, String>> map = new LinkedHashMap<>();
        map.put(recordId.getValue(),msg);

        for (Map.Entry<String, RedisConsumer> redisConsumerEntry : redisConsumer.entrySet()) {

            RedisConsumer redisConsumer = redisConsumerEntry.getValue();
            // 获取目标类
            Class<?> targetClass = AopUtils.getTargetClass(redisConsumer);
            RedisStream redisStream =
                    targetClass.getAnnotation(RedisStream.class);
            if (Objects.isNull(redisStream)) {
                continue;
            }
            String consumerStreamName = redisStream.streamName();

            if (!Objects.equals(streamName, consumerStreamName)) {
                continue;
            }

            String consumerGroupName = redisStream.groupName();

            RedisMsg redisMsg = new RedisMsg();
            redisMsg.setStreamName(streamName);
            redisMsg.setMsg(map);
            try {
                redisConsumer.getMessage(redisMsg);
                //逻辑处理完成后，ack消息，删除消息，group为消费组名称
                redisStreamUtil.ack(streamName, consumerGroupName, recordId.getValue());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        log.info("【streamName】= " + streamName + ",【recordId】= " + recordId + ",【msg】=" + msg);
    }
}



