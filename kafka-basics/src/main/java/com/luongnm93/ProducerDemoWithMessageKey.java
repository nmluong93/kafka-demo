package com.luongnm93;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithMessageKey {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithMessageKey.class);

    static void main(String[] args)  {
        logger.info("I am a Kafka Producer!");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int j = 0; j < 3; j++) {
                String key = "key_" + j;
                for (int i = 0; i < 1_000; i++) {
                    String value = "hello world " + i;
                    ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", key, value);
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logger.info(exception.getMessage());
                        } else {
                            logger.info("Key: {}, Partition: {}", key, metadata.partition());
                        }
                    });
                }
            }
            producer.flush();
        }

        logger.info("Producer sent message successfully.");
    }
}
