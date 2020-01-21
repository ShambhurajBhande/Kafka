package com.github.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThread {
    public static void main(String[] args) {
        new ConsumerWithThread().ConsumerStart();
    }

    public ConsumerWithThread() {
    }


    public void ConsumerStart(){
        Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class);
        logger.info("Creating consumer");
        CountDownLatch latch =new CountDownLatch(1);
        Runnable runnable=new ConsumerThread(latch);
        Thread thread=new Thread(runnable);
        thread.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown Hook...");
                ((ConsumerThread) runnable).shutDown();
                }
            ));
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.info("Application interrupted....");
            }finally {
                logger.info("Application is closing..");
            }
    }


    public class ConsumerThread implements Runnable{
        private Logger logger= LoggerFactory.getLogger(ConsumerThread.class);

        CountDownLatch latch;
        KafkaConsumer<String,String> kafkaConsumer;

        public ConsumerThread(CountDownLatch latch) {
            this.latch= latch;
            Properties properties = getProperties();
            kafkaConsumer=new KafkaConsumer<String, String>(properties);
            kafkaConsumer.subscribe(Arrays.asList("first_topic"));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        logger.info("Key: " + record.key() + ", " + "Value:" + record.value());
                        logger.info("Partition: " + record.partition() + ", " + "OffSet:" + record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received shutdown signal!!!");
            }finally {
                kafkaConsumer.close();
                latch.countDown();
            }
        }

        private Properties getProperties() {
            Properties properties=new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"My_Consumer");
            return properties;
        }

        public void shutDown(){
            kafkaConsumer.wakeup();
        }
    }
}
