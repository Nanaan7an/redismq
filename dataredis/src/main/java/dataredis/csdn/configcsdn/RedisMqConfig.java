package dataredis.csdn.configcsdn;

import lombok.Data;

/**
 * @Author Nanaan
 * @Date 2025/2/15 23:34
 * @Description TODO
 */

@Data
public class RedisMqConfig {
    private String streamName;
    private String groupName;
    private String consumerName;
}

