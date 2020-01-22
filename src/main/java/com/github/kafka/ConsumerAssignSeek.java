package com.github.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAssignSeek {
   static Logger logger= LoggerFactory.getLogger(ConsumerAssignSeek.class);

    public static void main(String[] args) {
        String topic="first_topic";
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String, String>(properties);

        TopicPartition topicPartition=new TopicPartition(topic,2);
        kafkaConsumer.assign(Arrays.asList(topicPartition));

        kafkaConsumer.seek(topicPartition,15L);

        while(true){
            ConsumerRecords<String,String> consumerRecords=kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record:consumerRecords){
                logger.info("Key: "+record.key()+", "+"Value:"+record.value());
                logger.info("Partition: "+record.partition()+", "+"OffSet:"+record.offset());
            }
        }
    }
}
