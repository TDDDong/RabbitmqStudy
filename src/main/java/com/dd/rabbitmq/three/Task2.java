package com.dd.rabbitmq.three;

import com.dd.rabbitmq.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.util.Scanner;

/**
 * 1.消息手动应答时不丢失
 * 2.放回队列后要可以重新消费 (模式工作线程2宕机 则被该线程接受但未ack的消息会重新回到队列中)
 */
public class Task2 {
    public static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        boolean durable = true;
        channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);
        Scanner scanner = new Scanner(System.in);
        while(scanner.hasNext()) {
            String message = scanner.next();
            //设置发送消息为持久化（保存到磁盘） 默认保存到内存中
            channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes("UTF-8"));
            System.out.println("生产者发出消息:" + message);
        }
    }
}
