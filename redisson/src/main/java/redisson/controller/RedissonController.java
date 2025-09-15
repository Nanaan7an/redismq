package redisson.controller;

import com.example.demo.config.redis.redisson.common.RedissonStreamUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author Nanaan
 * @Date 2025/2/18 14:59
 * @Description TODO
 */
@RestController
public class RedissonController {
    @Autowired
    RedissonStreamUtils redissonStreamUtils;

    @RequestMapping("/test/redisson")
    public void test() {
        String key = "java";
        redissonStreamUtils.createStream(key);

        Map<String, Object> data = new HashMap<String, Object>() {{
            put("hello", "hellojava");
            put("type", "java");
        }};
        String msgid = redissonStreamUtils.addMsg(key, data);
        System.out.println("消息id=" + msgid);


        redissonStreamUtils.createGroup(key, "g1");
        redissonStreamUtils.createGroup(key, "g2");
    }
}
