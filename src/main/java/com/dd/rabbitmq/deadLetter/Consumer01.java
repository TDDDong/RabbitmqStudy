package com.dd.rabbitmq.deadLetter;

import com.dd.rabbitmq.utils.RabbitMqUtils;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.util.HashMap;
import java.util.Map;

public class Consumer01 {
    //普通交换机
    public static final String NORMAL_EXCHANGE = "normal_exchange";
    //死信交换机
    public static final String DEAD_EXCHANGE = "dead_exchange";
    //普通队列
    public static final String NORMAL_QUEUE = "normal_queue";
    //死信队列
    public static final String DEAD_QUEUE = "dead_queue";

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        //声明死信和普通交换机  类型为direct
        channel.exchangeDeclare(NORMAL_EXCHANGE, BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare(DEAD_EXCHANGE, BuiltinExchangeType.DIRECT);
        //声明普通队列
        /**
         * 普通队列需要设置有关死信消息的参数 当满足条件时 消息才会被发送到死信交换机中
         */
        Map<String, Object> arguments = new HashMap<>();
        /**
         * 模拟第一种死信消息情况：超过ttl时间未被消费的消息成为死信消息
         * 设置过期时间 （可由生产者定义）
         */
        //arguments.put("x-message-ttl", 10000);
        //正常队列设置死信交换机
        arguments.put("x-dead-letter-exchange", DEAD_EXCHANGE);
        //设置死信routingKey
        arguments.put("x-dead-letter-routing-key", "lisi");
        /**
         * 模拟第二种死信消息情况：超过队列最大长度的消息成为死信消息
         * 设置队列最大长度为6
         */
        //arguments.put("x-max-length", 6);
        channel.queueDeclare(NORMAL_QUEUE, false, false, false, arguments);
        // ===============================================================================
        //声明死信队列
        channel.queueDeclare(DEAD_QUEUE, false, false, false, null);
        //绑定普通交换机与普通队列
        channel.queueBind(NORMAL_QUEUE, NORMAL_EXCHANGE, "zhangsan");
        //绑定死信交换机与死信队列
        channel.queueBind(DEAD_QUEUE, DEAD_EXCHANGE, "lisi");
        System.out.println("等待接收消息......");

        DeliverCallback deliverCallback = (consumerTag, message) -> {
            String msg = new String(message.getBody(), "UTF-8");
            if (msg.equalsIgnoreCase("info:5")) {
                System.out.println("Consumer01接收到的消息：" + msg + ": 此消息是被C1拒绝的");
                /**
                 * 模拟第三种情况： 拒绝接收信息 且该消息不重复入正常队列 则该消息会成为死信消息
                 */
                channel.basicReject(message.getEnvelope().getDeliveryTag(), false);
            } else {
                System.out.println("Consumer01接收到的消息：" + msg);
                //第二个参数为multiple 代表是否批量确认
                channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
            }
        };
        //开启手动应答 模拟拒绝情况
        channel.basicConsume(NORMAL_QUEUE, false, deliverCallback, consumerTag -> {});
    }
}
