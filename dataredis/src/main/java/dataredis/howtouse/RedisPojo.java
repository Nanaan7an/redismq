package dataredis.howtouse;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @Author Nanaan
 * @Date 2025/2/13 15:37
 * @Description 可以存入Redis的实体类，必须继承Serializable，否则无法存入
 */
@Data
public class RedisPojo implements Serializable {

    private String ip;

    private int port;

    private List<String> user;
}
