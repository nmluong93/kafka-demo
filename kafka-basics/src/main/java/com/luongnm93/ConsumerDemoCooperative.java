package com.luongnm93;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Graceful Shutdown — IntelliJ Gotcha
 *
 * When the build tool is set to Gradle (default), IntelliJ's stop button sends SIGKILL,
 * which bypasses JVM shutdown hooks entirely. Two workarounds:
 *
 * Option 1 — Terminal (SIGTERM):
 *   Run `jps` to find the PID, then `kill -15 <pid>`.
 *   SIGTERM triggers the shutdown hook → wakeup() → WakeupException → clean close.
 *
 * Option 2 — IntelliJ setting (recommended):
 *   Settings > Build, Execution, Deployment > Build Tools > Gradle
 *   Set "Run tests using" and "Build and run using" to "IntelliJ IDEA".
 *   The stop button then shows an exit icon that sends SIGTERM instead of SIGKILL.
 */
public class ConsumerDemoCooperative {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoCooperative.class);

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-java-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Shutdown hook: triggered on SIGTERM / Ctrl+C — wakeup() causes poll() to throw WakeupException
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown detected, calling consumer.wakeup()...");
            consumer.wakeup();
            try {
                mainThread.join(); // wait for main thread to finish processing + close
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try (consumer) {
            consumer.subscribe(List.of("demo_java"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
               if(!records.isEmpty()) logger.info("{} records received.", records.count());
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key: {}, Value: {}", record.key(), record.value());
                    logger.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                }
            }
        } catch (WakeupException e) {
            logger.info("Consumer woken up — initiating graceful shutdown.");
        } finally {
            logger.info("Consumer closed. Offsets committed.");
        }
    }
}
