package com.havrulyk.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static void main(String[] args) {
        //Create and set producers properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:19092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //Create producer record
        //Send data
        for(int i = 0; i < 10; i++){
            String topic = "demo_java";
            String value = "hello world[" + i + "]";
            String key = "id_" + i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            producer.send(producerRecord, (metadata, exception) -> {
                if(exception == null){
                    log.info("Key: " + key + " | " + "Partition: " + metadata.partition());
                }
            });
        }
        //Flush and close the producer
        producer.flush();
        producer.close();
    }
}
