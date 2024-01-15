package com.havrulyk.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerDemoGracefulShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoGracefulShutdown.class);

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

        //Get a reference for current thread
        final Thread mainThread = Thread.currentThread();


        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run(){
                log.info("Detected a shutdown, calling consumer.wakeup()...");
                consumer.wakeup();


                //join the main thread to allow the execution of code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try{
            //Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            while(true){
                //poll for data
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + " Value: " + record.value());
                    log.info("Partition: " + record.partition() + " Offset: " + record.offset());
                }
            }
        }catch (WakeupException e){
            log.info("Consumer is shutting down");
        }catch (Exception e){
            log.error("Unexpected exception in consumer " + e.getLocalizedMessage());
        }finally {
            consumer.close();
            log.info("The consumer has been gracefully shut down.");
        }

    }
}
