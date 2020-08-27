package com.github.indrajitb.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.datatransfer.FlavorTable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();

    }

    private void run() {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "third_topic";

        //latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //create the runnable
        //start the thread
        CosumerRunnable consumerRunnable = new CosumerRunnable(bootstrapServers, topic, groupId, latch);
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            consumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                logger.info("Application has exited.");
            }}));

        try {
            latch.await();
        }
        catch (InterruptedException e) {
            logger.error("Application got interrupted, e");
        }
        finally {
            logger.info("Application is closing.");
        }
    }

    public class CosumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(CosumerRunnable.class.getName());

        public CosumerRunnable(String bootstrapServers, String topic, String groupId, CountDownLatch latch) {
            this.latch = latch;

            //create Consumer properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create Consumer
            consumer = new KafkaConsumer<>(properties);

            //subscribe consumer to topic(s)
            consumer.subscribe(Arrays.asList(topic));

        }

        @Override
        public void run() {
            //poll for new data
            try {
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                    }
                }
             }
            catch (WakeupException e) {
                logger.info("Received shutdown signal.");
            }
            finally {
                consumer.close();
                //tell main code we're done
                latch.countDown();
            }

        }

        public void shutdown() {
            //...interrupt consumer.poll()
            //...will throw WakeupException
            consumer.wakeup();
        }
    }
}
