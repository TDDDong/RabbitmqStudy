package com.dd.rabbitmq.three;

import com.dd.rabbitmq.utils.RabbitMqUtils;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

public class Worker03 {
    public static final String TASK_QUEUE_NAME = "task_queue";

    /**
     * 开启两个进程 都用于消费消息
     * 测试结果： 轮询 接收消息并消费
     */
    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        System.out.println("C1等待接收消息 处理时间较短");
        DeliverCallback deliverCallback = (consumerTag, message) -> {
            try {
                // 沉睡1s
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("接收到的消息：" + new String(message.getBody(), "UTF-8"));

            /**
             * 1.消息的标记 tag
             * 2.是否批量应答 false：不批量应答信道中的消息 true：批量应答
             */
            channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
        };
        //设置不公平分发原则（能者多劳）
        //int prefetchSize = 1;
        //设置预取值为2
        int prefetchSize = 2;
        channel.basicQos(prefetchSize);
        //采用手动应答
        boolean autoAck = false;
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, deliverCallback, (consumerTag) -> {
            System.out.println(consumerTag + "消费者取消消费接口回调逻辑");
        });
    }
}
