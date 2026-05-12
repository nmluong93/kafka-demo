package com.luongnm93;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    static void main() throws InterruptedException {

        String bootStrapServer = "localhost:9092";
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * For Kafka client <= 2.8 only
         *          properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
         *         properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
         *         properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
         */


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        String topic = "wikimedia.recentChange";

        EventHandler eventHandler = new WikimediaChangeHandler(kafkaProducer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url))
                .headers(Headers.of("User-Agent", "Kafka-Wikimedia-Client/1.0 (https://example.com)"));
        EventSource eventSource = builder.build();
        eventSource.start();

        TimeUnit.SECONDS.sleep(10);
    }
}
