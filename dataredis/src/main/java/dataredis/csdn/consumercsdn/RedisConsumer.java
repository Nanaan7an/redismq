package dataredis.csdn.consumercsdn;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface RedisConsumer {

    /**
     * 转成实体类
     * 注： redis stream存取出来的数据有问题
     *
     * @param redisMsg 消息对象
     */
    default List<Dog> getMessage(RedisMsg redisMsg) throws InterruptedException {

        Map<String, Map<String, String>> map = redisMsg.getMsg();

        Collection<Map<String, String>> values = map.values();
        // 这里封装得一般 可以再优化
        List<Dog> res = new ArrayList<>();
        for (Map<String, String> msg : values) {
           Dog obj = null;
            try {
                // 遍历 Map 并尝试反序列化每个值
                for (Map.Entry<String, String> entry : msg.entrySet()) {

                    String json = entry.getValue();
//                    JSONObject jsonObject = JSONObject.parseObject(json);
//
//                    JSONArray jsonArray = JSONArray.parseArray(json);
//                    // 动态加载实体类
//                    Class<?> entityClass = Class.forName(String.valueOf(jsonArray.get(0)));
//                    String jsonString = JSON.toJSONString(jsonArray.get(1));
                    // 将数据转换为实体类对象
                    obj = JSON.parseObject(json,Dog.class);
                    // 设置消息id用于业务去重
                    //                    obj.setMsgId();
                    res.add(obj);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    default List<Dog> fallBack(RedisMsg redisMsg) {

        Map<String, Map<String, String>> map = redisMsg.getMsg();

        Collection<Map<String, String>> values = map.values();

        List<Dog> res = new ArrayList<>();
        for (Map<String, String> msg : values) {
            Dog obj = null;
            try {
                // 遍历 Map 并尝试反序列化每个值
                for (Map.Entry<String, String> entry : msg.entrySet()) {

                    String json = entry.getValue();
//                    JSONArray jsonArray = JSONArray.parseArray(json);
//                    // 动态加载实体类
//                    Class<?> entityClass = Class.forName(String.valueOf(jsonArray.get(0)));
//                    String jsonString = JSON.toJSONString(jsonArray.get(1));
                    // 将数据转换为实体类对象
                    obj = JSON.parseObject(json, Dog.class);
                    res.add(obj);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        return res;
    }
}

