package com.redismq.redismq.redis.dataredis.beagle.producer;

import com.redismq.redismq.redis.dataredis.beagle.config.BeaglProperties;
import com.redismq.redismq.redis.dataredis.common.util.RedisStreamUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author Nanaan
 * @Date 2025/6/16 0:03
 * @Description
 */
@RestController
public class BeagleProducer {
    @Autowired
    RedisStreamUtils utils;

    @Autowired
    BeaglProperties properties;
    @PostMapping("/createBeagle")
    public void createBeagle(String name){
        String streamKey = properties.getStream();
        String group = properties.getGroup();
        Map<String, String> msg = new HashMap<String,String>() {{
            put("name", name);
        }};


        System.out.println(utils.addMsg(streamKey, msg));
    }
}
