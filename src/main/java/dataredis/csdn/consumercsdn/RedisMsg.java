package dataredis.csdn.consumercsdn;

import lombok.Data;

import java.util.Map;

/**
 * @Author Nanaan
 * @Date 2025/2/15 23:37
 * @Description TODO
 */

@Data
public class RedisMsg {
    private String streamName;
    private Map<String, Map<String, String>> msg;
}
