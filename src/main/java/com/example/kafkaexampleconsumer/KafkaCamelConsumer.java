package com.example.kafkaexampleconsumer;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;


@Component
public class KafkaCamelConsumer extends RouteBuilder {

    static String TOPIC="topic-demo";

    @Override
    public void configure() throws Exception {
        from("kafka:" + TOPIC + "?brokers=192.168.1.74:9092" +
                "&groupId=group-1" +
                "&autoOffsetReset=latest"
        ).to("log:received-message-from-producer");
    }

}
