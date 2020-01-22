package com.github.kafka.elasticSearch;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
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

public class ElasticSearchConsumer {
    static Logger logger= LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient restHighLevelClient=createClient();

        KafkaConsumer<String ,String> kafkaConsumer= createConsumer();

        while(true){
            ConsumerRecords<String,String> consumerRecords=kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record:consumerRecords){
                IndexRequest indexRequest=new IndexRequest("kafka","topic").source(record.value(), XContentType.JSON);
                IndexResponse indexResponse=restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
                String id=indexResponse.getId();
                logger.info(id);
                Thread.sleep(1000);
            }
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
