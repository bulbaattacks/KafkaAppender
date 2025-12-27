package com.example.KafkaAppender.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerService {
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaProducerService(String topic, Properties props) {
        this.topic = topic;
        this.producer = new KafkaProducer<>(props);
    }

    public void send(String message) {
        producer.send(new ProducerRecord<>(topic, message));
    }

    public void close() {
        producer.flush();
        producer.close();
    }
}
