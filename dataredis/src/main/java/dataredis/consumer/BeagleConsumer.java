package dataredis.consumer;

import dataredis.config.BeaglProperties;
import dataredis.common.util.RedisStreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

/**
 * @Author Nanaan
 * @Date 2025/6/15 22:49
 * @Description 给小比安装声带
 */
@Component
@Slf4j
public class BeagleConsumer implements StreamListener<String, MapRecord<String, String, String>> {

    @Autowired
    BeaglProperties properties;

    @Autowired
    RedisStreamUtils utils;

    @Override
    public void onMessage(MapRecord<String, String, String> entries) {
        log.info("有新的小比被创造出来了！！！！！");

        String name = entries.getValue().get("name");
        log.info("{}学会了wer", name);
        utils.readMag(
                properties.getStream(),
                properties.getGroup(),
                properties.getConsumer()
        );
    }
}
