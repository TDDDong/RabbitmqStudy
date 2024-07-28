package com.dd.rabbitmq.fanout;

import com.dd.rabbitmq.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogs02 {
    public static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        //创建交换机 (扇出)
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        //声明一个临时队列
        String queueName = channel.queueDeclare().getQueue();
        //绑定
        /**
         * 1.队列名称
         * 2.交换机名称
         * 3.routingKey
         *
         * fanout类型交换机 会直接忽略routingKey 将消息发送给所有队列
         */
        channel.queueBind(queueName, EXCHANGE_NAME, "456");
        System.out.println("等待接收消息,把接收到的消息打印在屏幕上......");

        DeliverCallback deliverCallback = (consumerTag, message) -> {
            System.out.println("ReceiveLogs02控制台打印接收到的消息：" + new String(message.getBody(), "UTF-8"));
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }
}
