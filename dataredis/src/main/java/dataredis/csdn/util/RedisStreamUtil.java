package dataredis.csdn.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamInfo;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @Author Nanaan
 * @Date 2025/2/15 23:15
 * @Description 封装使用Stream的基本方法
 */
@Component
public class RedisStreamUtil {
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    /**
     * 创建消费组
     *
     * @param key   键名称
     * @param group 组名称
     * @return {@link String}
     */
    public String createGroupCsdn(String key, String group) {
        return redisTemplate.opsForStream().createGroup(key, group);
    }

    /**
     * 获取消费者信息
     *
     * @param key   键名称
     * @param group 组名称
     * @return {@link StreamInfo.XInfoConsumers}
     */
    public StreamInfo.XInfoConsumers queryConsumers(String key, String group) {
        return redisTemplate.opsForStream().consumers(key, group);
    }

    /**
     * 查询组信息
     *
     * @param key 键名称
     * @return
     */
    public StreamInfo.XInfoGroups queryGroups(String key) {
        return redisTemplate.opsForStream().groups(key);
    }

    /**
     * 添加Map消息
     * 封装 XADD 命令
     *
     * @param key
     * @param value
     */
    public String addMap(String key, Map<String, Object> value) {
        return redisTemplate.opsForStream().add(key, value).getValue();
    }

    /**
     * 读取消息
     * 独立消费
     *
     * @param key
     */
    public List<MapRecord<String, Object, Object>> read(String key) {
        return redisTemplate.opsForStream().read(StreamOffset.fromStart(key));
    }

    /**
     * 确认消费
     *
     * @param key
     * @param group
     * @param recordIds
     */
    public Long ack(String key, String group, String... recordIds) {
        return redisTemplate.opsForStream().acknowledge(key, group, recordIds);
    }

    /**
     * 删除消息
     * 当一个节点的所有消息都被删除，那么该节点会自动销毁
     *
     * @param key
     * @param recordIds
     */
    public Long del(String key, String... recordIds) {
        return redisTemplate.opsForStream().delete(key, recordIds);
    }

    /**
     * 判断是否存在key
     *
     * @param key
     */
    public boolean hasKey(String key) {
        Boolean flag = redisTemplate.hasKey(key);
        return flag != null && flag;
    }

}

