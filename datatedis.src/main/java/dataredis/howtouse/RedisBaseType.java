package dataredis.howtouse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.*;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author Nanaan
 * @Date 2025/2/12 14:41
 * @Description SpringBoot使用Redis的五种基本数据类型
 */
@RestController
public class RedisBaseType {
    @Autowired
    RedisTemplate<String, String> redisTemplate;

    @RequestMapping("/use")
    public void use() {
        System.out.println("使用redis");
        /*
         * 字符串
         * ValueOperations<K, V>，K通常为String，V为所需的类型
         */
        ValueOperations<String, String> valueOperations = redisTemplate.opsForValue();
        valueOperations.set("str", "strValue");
        String strRes = redisTemplate.opsForValue().get("str");
        System.out.println(strRes);

        /*
         * 列表
         * 左右均可push、pop，自由搭配可实现FIFO、FILO
         * range()可以读取列表内容，需指定起止位置，[0,-1]读取全部值
         */
        ListOperations<String, String> listOperations = redisTemplate.opsForList();
        listOperations.leftPush("list", "listValue1");
        listOperations.leftPush("list", "listValue2");
        List<String> listRes = redisTemplate.opsForList().range("list", 0, 1);
        System.out.println(listRes);

        /*
        集合的值
         */
        SetOperations<String, String> setOperations = redisTemplate.opsForSet();
        setOperations.add("set", "setValue1", "setValue3", "setValue2");
        Set<String> setRes = setOperations.members("set");
        System.out.println(setRes);

        /*
        有序集合
         */
        ZSetOperations<String, String> zSetOperations = redisTemplate.opsForZSet();
        zSetOperations.add("zset", "zset1", 1);
        zSetOperations.add("zset", "zset3", 2);
        zSetOperations.add("zset", "zset2", 3);
        Set<String> zset = zSetOperations.range("zset", 0, -1);
        System.out.println(zset);

        /*
        哈希
        结构类似HashMap<String,HashMap<String,V>>
         */
        HashOperations<String, String, String> hashOperations = redisTemplate.opsForHash();
        hashOperations.put("hash", "hash1", "1");
        hashOperations.put("hash", "hash2", "2");
        hashOperations.put("hash", "hash3", "3");
        Map<String, String> hashMap = hashOperations.entries("hash");
        System.out.println(hashMap);
        System.out.println(hashMap.get("hash1").getClass());

    }
}
