package com.luongnm93;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) throws InterruptedException {
        logger.info("I am a Kafka Producer!");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");
//        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int j = 0; j < 10; j++) {
                for (int i = 0; i < 3; i++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "hello world " + (i + j));
                    producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception != null) {
                                logger.info(exception.getMessage());
                            } else {
                                String time = LocalDateTime.ofInstant(Instant.ofEpochMilli(metadata.timestamp()), ZoneId.systemDefault())
                                        .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
                                logger.info("Sent message successfully. \n\t topic={} \n\t partition={} \n\t offset={} \n\t message={} \n\t timestamp={}",
                                        metadata.topic(),
                                        metadata.partition(),
                                        metadata.offset(),
                                        record.value(),
                                        time);
                            }
                        }
                    });
                }
                producer.flush();
                Thread.sleep(500);
            }
        }

        logger.info("Producer sent message successfully.");
    }
}
