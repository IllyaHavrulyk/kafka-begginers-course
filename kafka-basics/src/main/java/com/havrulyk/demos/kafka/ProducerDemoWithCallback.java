package com.havrulyk.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world " + i);

            producer.send(producerRecord, (metadata, exception) -> {
                if(exception == null){
                    log.info("Received metadata\n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Timestamp: " + metadata.timestamp() + "\n" +
                            "Offset: " + metadata.offset() + "\n" );
                }
            });
        }
        //Flush and close the producer
        producer.flush();
        producer.close();
    }
}
