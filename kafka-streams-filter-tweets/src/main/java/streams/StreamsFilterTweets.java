package streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    private static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_topics");
        KStream<String, String> filteredStream = inputTopic.filter((k, jsonTweet) -> extractIdFromTweet(jsonTweet) > 10000);
        filteredStream.to("important_tweets");

        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        //start streams application
        kafkaStreams.start();
    }

    private static int extractIdFromTweet(String tweetJson) {
        try {
            return jsonParser.parse(tweetJson).getAsJsonObject().get("user")
                    .getAsJsonObject().get("followers_count").getAsInt();
        } catch(NullPointerException ex) {
            return 0;
        }
    }
}
