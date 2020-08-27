package com.github.indrajitb.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        String bootstrapServers = "127.0.0.1:9092";

        //Create Producer Configs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String>   producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; ++i) {
            String topic = "third_topic";
            String value = "Hello, Kafka ...to third_topic..." + Integer.toString(i) + "!!";
            String key = "id_" + Integer.toString(i);

            logger.info("\n" + "Key: " + key);
            //Create a Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

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
            }).get(); //block the .send() - sync call - not to be done in prod.
        }

        //flush and close producer
        producer.flush();
        producer.close();

    }
}
