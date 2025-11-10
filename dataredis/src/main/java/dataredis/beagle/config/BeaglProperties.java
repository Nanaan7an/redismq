package dataredis.beagle.config;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * @Author Nanaan
 * @Description 动态绑定，用于操作队列
 */
@Component
@Data
@PropertySource("classpath:dataredis/Beagle.properties")
@ConfigurationProperties("beagle")
public class BeaglProperties {
    private String stream;

    private String group;

    private String consumer;
}
