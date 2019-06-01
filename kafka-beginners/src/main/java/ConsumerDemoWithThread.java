import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public void run() {
        CountDownLatch latch = new CountDownLatch(1);
        Runnable runnable = new ConsumerThread(latch, "firsttopic", "localhost:9092", "my-app2");
        Thread thread = new Thread(runnable);
        thread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ((ConsumerThread) runnable).shutdown();
        }));
        try {
            latch.await();
        } catch (InterruptedException ex) {
            logger.error("Application got interrupted", ex);
        } finally {
            logger.info("Application is closing");
        }
    }

    class ConsumerThread implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private final Logger logger1 = LoggerFactory.getLogger(ConsumerThread.class);

        public ConsumerThread(CountDownLatch latch, String topic, String bootStrapServers, String groupId) {
            this.latch = latch;
            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton(topic));

        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger1.info("Key: {}, Value : {}, Partition: {}, Offset: {}",
                                record.key(), record.value(), record.partition(), record.offset());
                    }
                }
            } catch(WakeupException ex) {
                logger1.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
