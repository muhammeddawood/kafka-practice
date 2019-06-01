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
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
    public static void main(String[] args) {

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-seven-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        String topic = "firsttopic";

        // subscribe consumer to topic
        //consumer.subscribe(Collections.singleton("firsttopic"));

        // assign
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicPartition));

        consumer.seek(topicPartition, 15);

        // poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records) {
                logger.info("Key: {}, Value : {}, Partition: {}, Offset: {}",
                        record.key(), record.value(), record.partition(), record.offset());
            }

        }
    }
}
