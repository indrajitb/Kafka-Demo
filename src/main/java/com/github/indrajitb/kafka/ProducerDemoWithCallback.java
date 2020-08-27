package com.github.indrajitb.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers = "127.0.0.1:9092";

        //Create Producer Configs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String>   producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; ++i) {
            //Create a Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic",
                    "Hello, Kafka " + Integer.toString(i) + " !!");

            //Send Data - async
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        //the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    }
                    else {
                        //some error occured
                        logger.error("Error: ", e);
                    }
                }
            });
        }

        //flush and close producer
        producer.flush();
        producer.close();

    }
}
