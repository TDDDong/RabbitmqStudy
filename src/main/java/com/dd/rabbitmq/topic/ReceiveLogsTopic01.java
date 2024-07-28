package com.dd.rabbitmq.topic;

import com.dd.rabbitmq.utils.RabbitMqUtils;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogsTopic01 {
    public static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        //声明交换机
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        //创建队列
        String queueName = "Q1";
        channel.queueDeclare(queueName, false, false, false, null);
        /**
         *   “*”代表一个任何字符
         *   “#”代表0个或者多个任意字符
         */
        channel.queueBind(queueName, EXCHANGE_NAME, "*.orange.*");
        System.out.println("等待接收消息......");

        DeliverCallback deliverCallback = (consumerTag, message) -> {
            System.out.println(new String(message.getBody(), "UTF-8"));
            System.out.println("接收队列：" + queueName + " 绑定键：" + message.getEnvelope().getRoutingKey());
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }
}
