package com.github.indrajitb.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        //Create Producer Configs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String>   producer = new KafkaProducer<>(properties);

        //Create a Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello, Kafka!!");

        //Send Data - async
        producer.send(record);

        //flush and close producer
        producer.flush();
        producer.close();

    }
}
