package dataredis.config;

import lombok.Data;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * @Author Nanaan
 * @Date 2025/5/3 18:38
 * @Description 仅存在一组消息队列的情况下
 */
@Component
@Data
@PropertySource(value = "classpath:dataredis/singlemq.properties")
@ConfigurationProperties("redis.mq")
public class SingleMqProperties {
    private String streamName;

    private String groupName;

    private String consumerName;
}
