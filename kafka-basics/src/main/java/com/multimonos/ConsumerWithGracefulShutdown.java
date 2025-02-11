package com.multimonos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithGracefulShutdown {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        String groupId = "group0";
        String topic = "demojava_p5";

        log.info("consumer : start");

        // config
        Properties props = new Properties();

        // config.localhost
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // consumer config
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "earliest"); // none|earliest - entire history|latest (new only)

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        // get ref to main thread
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("consumer.shutdown : exit by calling consumer.wakeup()");
                consumer.wakeup(); // will throw exception on next poll

                // join the main thread
                try {
                    mainThread.join(); // wait for the infinite loop to finish
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });


        // inifinite loop
        try {

            while (true) {
                log.info("consumer : polling : topic={} ...", topic);

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("consumer.record: key={}, value={}, partition={}, offset={}", record.key(), record.value(), record.partition(), record.offset());
                }
            }

        } catch (WakeupException e) {
            log.info("consumer.shutdown : shutdown triggered ...");

        } catch (Exception e) {
            log.error("consumer.exception : {}", e.getMessage());

        } finally {
            consumer.close();  // close and commit offsets
            log.info("consumer.shutdown : graceful shutdown complete");
        }
    }
}

