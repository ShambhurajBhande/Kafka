package com.github.kafka.elasticSearch;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumerIdempotent {
    static Logger logger= LoggerFactory.getLogger(ElasticSearchConsumerIdempotent.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient restHighLevelClient=createClient();

        KafkaConsumer<String ,String> kafkaConsumer= createConsumer();

        while(true){
            ConsumerRecords<String,String> consumerRecords=kafkaConsumer.poll(Duration.ofMillis(100));
            logger.info("Record Count"+ consumerRecords.count());
            for (ConsumerRecord<String,String> record:consumerRecords){
                String id=record.topic()+"_"+record.partition()+"_"+record.offset();
                IndexRequest indexRequest=new IndexRequest("kafka","topic",id).source(record.value(), XContentType.JSON);
                IndexResponse indexResponse=restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
                logger.info(indexResponse.getId());
                Thread.sleep(1000);
            }
            logger.info("Committing offset!!!!");
            kafkaConsumer.commitSync();
            logger.info("Offset committed!!!!");
        }
    }

    public static KafkaConsumer<String, String> createConsumer(){
        String topic="elasticSearch_topic";
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"My_ElasticSearchConsumer");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"5");
        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        return kafkaConsumer;
    }

    private static RestHighLevelClient createClient() {
        String host="localhost";
        int port=9200;
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
