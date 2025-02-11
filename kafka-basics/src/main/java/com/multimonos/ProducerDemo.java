package com.multimonos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

// ---
// CONSUMER
// kafka-console-consumer --topic demo_java --from-beginning \
//  --formatter kafka.tools.DefaultMessageFormatter \
//  --property print.timestamp=true \
//  --property print.key=true \
//  --property print.value=true \
//  --property print.partition=true \
//  --bootstrap-server localhost:9092
// ---
public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("producer : start");

        // config
        Properties props = new Properties();

        // config.localhost
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // create producer
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        // create msg
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord("demo_java", "helloworld2");

        // send msg
        producer.send(producerRecord);

        // flush and close sync
        producer.flush();
        producer.close();
    }
}
