package dataredis.beagle.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dataredis.beagle.config.BeaglProperties;
import dataredis.beagle.config.BeagleParams;
import dataredis.util.RedisStreamUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
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

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 简单地测试一下
     * 使用浏览器访问http://localhost:8080/createBeagle?name=字符串
     * @param name 将作为小比的名字
     */
    @GetMapping("/createBeagle")
    public void createBeagle(String name){
        String streamKey = properties.getStream();
        BeagleParams beagleParams=new BeagleParams();
        beagleParams.setName(name);
        System.out.println(utils.addMsg(streamKey, objectMapper.convertValue(beagleParams,Map.class)));
    }
}
