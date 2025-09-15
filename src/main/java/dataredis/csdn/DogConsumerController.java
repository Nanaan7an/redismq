package dataredis.csdn;

import dataredis.csdn.consumercsdn.Dog;
import dataredis.csdn.consumercsdn.RedisConsumer;
import dataredis.csdn.consumercsdn.RedisMsg;
import dataredis.csdn.consumercsdn.RedisStream;
import dataredis.csdn.util.RedisStreamUtil;
import dataredis.common.util.RedisStreamUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RedisStream(streamName = "dogs", groupName = "dogGroup")
@RestController
public class DogConsumerController implements RedisConsumer {

    @Autowired
    private RedisStreamUtil redisStreamUtil;

    @Autowired
    private RedisStreamUtils redisStreamUtils;

    @RequestMapping("/stream")
    public String testStream() {

        String mystream = "";
        Dog dog = new Dog();
        dog.setName("dog");
        Map<String, Object> map = new HashMap<>();
        map.put("dog", dog);
        mystream = redisStreamUtil.addMap("dogs", map);
        System.out.println("生产者" + redisStreamUtil.queryConsumers("dogs", "dogGroup"));

        Map<String, String> cat = new HashMap<>();
        cat.put("catk", "cat3");
        System.out.println("cat addMsg"+redisStreamUtils.addMsg("cat", cat));
        redisStreamUtils.getGroup("cat","testcat");

        return String.valueOf(mystream);
    }

    /**
     * 模拟消费操作
     *
     * @param redisMsg 消息对象
     * @return
     */
    @Override
    public List<Dog> getMessage(RedisMsg redisMsg) throws InterruptedException {
        System.out.println("收到了狗狗消息=============" + redisMsg.getMsg());
        List<Dog> message = RedisConsumer.super.getMessage(redisMsg);
        System.out.println("dog收到消息 准备ack:" + message);
        return message;
    }

    /**
     * 模拟降级操作 (从pending list获取消息)
     *
     * @param redisMsg
     * @return
     */
    @Override
    public List<Dog> fallBack(RedisMsg redisMsg) {
        List<Dog> message = RedisConsumer.super.fallBack(redisMsg);
        System.out.println("order完成了降级:" + message);
        return message;
    }
}


