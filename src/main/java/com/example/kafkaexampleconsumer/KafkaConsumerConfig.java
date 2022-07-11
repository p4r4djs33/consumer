package com.example.kafkaexampleconsumer;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerConfig {



    static String BOOTSTRAP_SERVERS = "192.168.1.74:9092";
    static String TOPIC = "topic-demo";
    static String CONSUMER_GROUP_ID = "group-1";
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(KafkaConsumerConfig.class);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Arrays.asList(TOPIC));
        System.out.println(Arrays.asList(TOPIC));


        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        while (true) {
            ConsumerRecords<String, String> records =consumer.poll(Duration.ofMillis(5000));

            for (ConsumerRecord consumerRecord : records) {
                logger.info("Receiver new record: \n" +
                        "Key: " + consumerRecord.key() +
                        ", Value: " + consumerRecord.value() +
                        ", Topic: " + consumerRecord.topic() +
                        ", Partition: " + consumerRecord.partition() +
                        ", Offset: " + consumerRecord.offset());
                currentOffsets.put(
                        new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                        new OffsetAndMetadata(consumerRecord.offset()+1)
                );
                consumer.commitSync(currentOffsets);
            }


        }


    }
}

