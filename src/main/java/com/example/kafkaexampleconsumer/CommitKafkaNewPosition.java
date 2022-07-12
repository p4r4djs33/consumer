package com.example.kafkaexampleconsumer;

import org.apache.kafka.clients.admin.KafkaAdminClient;
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


    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(KafkaConsumerConfig.class);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        ConsumerRecords<String, String> records;

        TopicPartition tp = new TopicPartition(TOPIC, 2);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // Move to the desired start offset

                    consumer.seek(tp, 0L);
                }
            });
            boolean run = true;
/*            long lastOffset = 20L;*/
            java.sql.Timestamp ts = java.sql.Timestamp.valueOf("2022-07-12 09:25:47.934");
            Long tsLong = ts.getTime();
            while (run) {
                ConsumerRecords<String, String> crs = consumer.poll(Duration.ofMillis(100L));
                for (ConsumerRecord<String, String> record : crs) {
                    if (record.timestamp() >= tsLong) {
                        System.out.println(record);
                    }
                    /*if (record.offset() == lastOffset) {
                        // Reached the end offsey, stop consuming
                        run = false;
                        break;
                    }*/
                    if (crs.isEmpty()) {
                        run = false;
                        break;
                    }
                }
/*                if (crs.isEmpty()) {
                    System.out.println("-- terminating consumer --");
                    break;
                }*/

            }
        }


    }

}


