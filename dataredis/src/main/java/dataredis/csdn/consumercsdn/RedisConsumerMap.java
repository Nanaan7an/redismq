package dataredis.csdn.consumercsdn;

import lombok.Data;

import java.util.Map;

/**
 * @Author Nanaan
 * @Date 2025/2/16 0:01
 * @Description TODO
 */
//@Component
@Data
public class RedisConsumerMap {
    private Map<String, RedisConsumer> redisConsumer;
}
