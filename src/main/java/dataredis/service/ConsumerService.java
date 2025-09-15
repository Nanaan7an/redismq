package dataredis.service;

import dataredis.common.util.RedisStreamUtils;
import dataredis.config.SingleMqProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Author Nanaan
 * @Date 2025/5/7 13:17
 * @Description 业务需要的
 */
@Service
public class ConsumerService {

    @Autowired
    SingleMqProperties singleMqProperties;

    @Autowired
    RedisStreamUtils redisStreamUtils;

    public void service(){

        //消费消息
        redisStreamUtils.readMag(
                singleMqProperties.getStreamName(),
                singleMqProperties.getGroupName(),
                singleMqProperties.getConsumerName());
    }
}
