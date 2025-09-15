package dataredis.csdn.configcsdn;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @Author Nanaan
 * @Date 2025/2/15 23:34
 * @Description TODO
 */


@Data
@Component
@ConfigurationProperties(prefix = "redis.stream")
public class RedisMqProperties {

    private boolean enable;

    private List<RedisMqConfig> configs;


}

