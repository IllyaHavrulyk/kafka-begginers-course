package com.havrulyk.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        String topic = "demo_java";
        String groupId = "my-java-application";
        //Create and set consumers properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:19092");
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        //Create the Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(topic));

        consumer.poll(Duration.ofMillis(1000));

        while(true){
            log.info("Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + " Value: " + record.value());
                log.info("Partition: " + record.partition() + " Offset: " + record.offset());
            }
        }
    }
}
