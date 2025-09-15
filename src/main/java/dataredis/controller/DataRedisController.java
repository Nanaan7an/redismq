package dataredis.controller;

import dataredis.config.SingleMqProperties;
import dataredis.common.util.RedisStreamUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * Author:  Nanaan
 * Date 2025/2/28 23:21
 * Description TODO
 */
@RestController
public class DataRedisController {
    @Autowired
    RedisStreamUtils utils;

    @Autowired
    SingleMqProperties properties;

    @RequestMapping("/sinlgemq")
    public void testDataRedis(@RequestBody Map<String,String> map) {
        String streamKey = properties.getStreamName();
        String group = properties.getGroupName();
        Map<String, String> msg = new HashMap<String,String>() {{
            put(map.get("tag"), map.get("value"));
        }};

        System.out.println(utils.addMsg(streamKey, msg));
    }

    @RequestMapping("/consumer")
    public void testConsumer(){
        utils.readMag(properties.getStreamName(), properties.getGroupName(), properties.getConsumerName());
    }
}
