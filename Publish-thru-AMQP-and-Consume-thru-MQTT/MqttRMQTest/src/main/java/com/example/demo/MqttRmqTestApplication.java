package com.example.demo;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.lifecycle.MqttClientDisconnectedContext;
import com.hivemq.client.mqtt.lifecycle.MqttClientDisconnectedListener;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck;
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAckReturnCode;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.Mqtt3Subscribe;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


@SpringBootApplication
@Slf4j
public class MqttRmqTestApplication {


    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException, TimeoutException {

        byte[] payload = "This payload-1 was published to RMQ".getBytes();
        byte[] payload2 = "This payload-2 was published to RMQ".getBytes();
        String topic = "device.11";
        String topic2 = "device.22";
        MqttQos qos = MqttQos.AT_LEAST_ONCE;
        ConnectionFactory factory=null, factory1=null;


        SpringApplication.run(MqttRmqTestApplication.class, args);

        //MQTT client
        Mqtt3AsyncClient client = MqttClient.builder()
                .useMqttVersion3().addDisconnectedListener(new MqttClientDisconnectedListener() {
                @Override
                public void onDisconnected(MqttClientDisconnectedContext context) {
                    log.info("mqtt client disconnected : " + context.getCause().toString(),
                            context.getCause());
                    }
                })
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(1883)
                .buildAsync();

        //connect MQTT client to RMQ
        try {
            Mqtt3ConnAck connAck = client.connectWith()
                    .simpleAuth()
                    .username("guest")
                    .password("guest".getBytes())
                    .applySimpleAuth()
                    .send()
                    .get();

            if (connAck.getReturnCode().equals(Mqtt3ConnAckReturnCode.SUCCESS)) {
                log.info(" mqtt client connected successfully!");
            } else {
                log.error("mqtt client failed to connect: " + connAck.getReturnCode().toString());
            }
        } catch(Exception e) {
            log.error(" exception in connection ==> ", e);
        }

//        //subscribe to topic
//        client.subscribeWith().topicFilter("22")
//                .qos(qos)
//                .callback(pub ->{
//                        log.info("Message received:"+new String(pub.getPayloadAsBytes()));
//                }).send()
//                .whenComplete((subAck, throwable) -> {
//                    log.info(subAck.getReturnCodes().toString());
//                    if (throwable != null) {
//                        log.error("Failed to subscribe : "+throwable.getMessage());
//                    } else {
//                        log.info("subscribed successfully");
//                    }
//                });
//        client.subscribeWith().topicFilter("11")
//                .qos(qos)
//                .callback(pub ->{
//                    log.info("Message received:"+new String(pub.getPayloadAsBytes()));
//                }).send()
//                .whenComplete((subAck, throwable) -> {
//                    log.info(subAck.getReturnCodes().toString());
//                    if (throwable != null) {
//                        log.error("Failed to subscribe : "+throwable.getMessage());
//                    } else {
//                        log.info("subscribed successfully");
//                    }
//                });

        //Thread.sleep(5000);

        //publish
        client.publishWith()
                .topic(topic)
                .payload(payload)
                .qos(qos)
                .send()
                .whenComplete((mqtt3Publish, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to publish");
                    } else {
                        log.info("Published successfully!");
                    }
                });

        client.publishWith()
                .topic(topic2)
                .payload(payload2)
                .qos(qos)
                .send()
                .whenComplete((mqtt3Publish, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to publish");
                    } else {
                        log.info("Published successfully!");
                    }
                });

        //AMQP Consumer-1
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare("QUEUE-1", true, false, false, null);

        channel.queueBind("QUEUE-1", "amq.topic", "device.*");


        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received - 1'" + message + "'");
        };
        channel.basicConsume("QUEUE-1", true, deliverCallback, consumerTag -> { });



        //AMQP Consumer-2
        factory1 = new ConnectionFactory();
        factory1.setHost("localhost");
        Connection connection1 = factory1.newConnection();
        Channel channel1 = connection1.createChannel();

        channel1.queueDeclare("QUEUE-2", true, false, false, null);

        channel1.queueBind("QUEUE-2", "amq.topic", "device.*");


        DeliverCallback deliverCallback1 = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received - 2'" + message + "'");
        };
        channel1.basicConsume("QUEUE-2", true, deliverCallback1, consumerTag2 -> { });


    }

}



