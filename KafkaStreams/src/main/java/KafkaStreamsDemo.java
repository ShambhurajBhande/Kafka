import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamsDemo {

    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"kafka-streams");

        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream<String,String> inputTopic=streamsBuilder.stream("stream_input");
        KStream<String,String> filteredStream=inputTopic.filter((k,inputJson)->extractUserAge(inputJson)>5);

        filteredStream.to("stream_output");

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);
        kafkaStreams.start();

    }

    private static JsonParser  jsonParser=new JsonParser();

    private static int extractUserAge(String inputJson) {
        return jsonParser.parse(inputJson).getAsJsonObject().get("age").getAsInt();
    }
}
