package com.example.kafkaexampleconsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class CommitKafkaNewPosition {

    static String BOOTSTRAP_SERVERS = "192.168.1.74:9092";
    static String TOPIC = "topic-6";
    static String CONSUMER_GROUP_ID = "group-6";

    private static KafkaConsumer<String, String> consumer;
    private static TopicPartition topicPartition;

    private static Collection<TopicPartition> topicPartitions;

    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    private static Long toOffset;

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(KafkaConsumerConfig.class);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        /*        String string = "2";
        String[] partitionArray= string.split(",");
        for (String s : partitionArray) {
            TopicPartition topicPartitionNew = new TopicPartition(TOPIC, Integer.parseInt(s));
            topicPartitions.add(topicPartitionNew);
        }
        consumer.assign(topicPartitions);*/
        ConsumerRecords<String, String> records;
        consumer = new KafkaConsumer<String, String>(properties);
        topicPartitions = new ArrayList<>();
        topicPartition = new TopicPartition(TOPIC, 2);
        topicPartitions.add(topicPartition);
        consumer.assign(topicPartitions);
        currentOffsets = new HashMap<>();
        currentOffsets.put(topicPartition, new OffsetAndMetadata(15));
        consumer.commitSync(currentOffsets);
        toOffset = 20L;
        boolean run = true;

            records = consumer.poll(Duration.ofMillis(5000));
            for (ConsumerRecord consumerRecord : records) {
                if (consumerRecord.offset() <= toOffset) {
                    logger.info("Receiver new record: \n" + "Key: " + consumerRecord.key() + ", Value: " + consumerRecord.value() + ", Topic: " + consumerRecord.topic() + ", Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset());
                }
                topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                currentOffsets.put(topicPartition, new OffsetAndMetadata(consumerRecord.offset() + 1));
                consumer.commitSync(currentOffsets);
            }


//==========================================================================
/*
        consumer = new KafkaConsumer<String, String>(properties);
        topicPartitions = new ArrayList<>();
        topicPartition = new TopicPartition(TOPIC, 1);
        topicPartitions.add(topicPartition);
        consumer.assign(topicPartitions);
        currentOffsets = new HashMap<>();
        currentOffsets.put(topicPartition, new OffsetAndMetadata(5));
        consumer.commitSync(currentOffsets);
        toOffset = 10L;
        boolean run2 = true;
        while (run2) {
            records = consumer.poll(Duration.ofMillis(5000));
            for (ConsumerRecord consumerRecord : records) {
                if (consumerRecord.offset() <= toOffset) {
                    logger.info("Receiver new record: \n" + "Key: " + consumerRecord.key() + ", Value: " + consumerRecord.value() + ", Topic: " + consumerRecord.topic() + ", Partition: " + consumerRecord.partition() + ", Offset: " + consumerRecord.offset());
                }
                topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
                currentOffsets.put(topicPartition, new OffsetAndMetadata(consumerRecord.offset() + 1));
                consumer.commitSync(currentOffsets);

                if (consumerRecord.offset() == toOffset) {
                    run2 = false;
                    break;
                }
            }
        }*/
    }

}


