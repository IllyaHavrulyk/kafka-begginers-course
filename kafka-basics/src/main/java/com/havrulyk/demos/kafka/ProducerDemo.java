package com.havrulyk.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        //Create and set producers properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:19092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //Create producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");
        //Send data
        producer.send(producerRecord);
        //Flush and close the producer
        producer.flush();
        producer.close();
    }
}
