package com.havrulyk.demos.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaRecentChangesEventHandler implements EventHandler {
    private KafkaProducer<String, String> producer;
    private String topic;
    private final Logger logger = LoggerFactory.getLogger(WikimediaRecentChangesEventHandler.class.getSimpleName());

    public WikimediaRecentChangesEventHandler(KafkaProducer<String, String> producer, String topic){
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen(){
        //nothing here
    }

    @Override
    public void onClosed(){
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        logger.info("New change in wikimedia: " + messageEvent.getData());
        //asynchronous
        producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) throws Exception {
        //nothing here
    }

    @Override
    public void onError(Throwable t) {
        logger.error("Error in stream reading: " + t.getLocalizedMessage());
    }
}
