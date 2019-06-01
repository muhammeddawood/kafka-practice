package tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    String consumerKey = "8xfPk5j9Ws6wAZmAXgPEzuyS6";
    String consumerSecret = "RUtF7FUjLyYDKo2a3LrXaKkbc1hhW2tobkyDfF15NmJXGHTiZC";
    String token = "980103828769988608-ZO8DigEMhvy461Y8fivagPisTl4Us4L";
    String secret = "AY7D76C77skhInO0STUCyWbYW8mnHNJh8JtGsEHLFMj37";
    List<String> terms = Lists.newArrayList("bitcoin");

    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    private void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        // create a twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        //create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // loop to send tweets to kafka

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shuting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done");
        }));

        while (!client.isDone()) {
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                something(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (metadata, ex) -> {
                    if(ex != null) {
                        logger.error(ex.getLocalizedMessage(), ex);
                    }
                });
                profit();
            } catch(InterruptedException ex) {
                logger.error(ex.getLocalizedMessage(), ex);
                client.stop();
            }
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    private void profit() {
    }

    private void something(String msg) {
        System.out.println(msg);
    }


    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);

        hosebirdEndpoint.trackTerms(terms);


        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey,
                consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
