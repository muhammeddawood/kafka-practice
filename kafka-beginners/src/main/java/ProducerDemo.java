import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>("firsttopic", "hi");

        for(int i = 0; i < 10; i++) {
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("Received new metadata: \n " +
                                "Topic: {} \n " +
                                "Partition: {} \n" +
                                "Offset: {} \n" +
                                "Timestamp: {}", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }

}
