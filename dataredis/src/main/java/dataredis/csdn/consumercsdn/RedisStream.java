package dataredis.csdn.consumercsdn;

import org.springframework.stereotype.Component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Component
public @interface RedisStream {

    /**
     * 消息主题
     * 不设计数组: 1. 接口应遵循单一职责 2.数组带来ack应答问题
     */
    String streamName() ;

    /**
     * 消费组
     * 不设计数组: 1. 接口应遵循单一职责 2.数组带来ack应答问题
     */
    String groupName();

    /**
     * 消费者 ack按组应答 不使用消费者概念 增大acK控制难度
     */


}

