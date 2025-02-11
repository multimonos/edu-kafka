package com.multimonos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * CONSUMER
 * kafka-console-consumer --topic demo_java --from-beginning \
 * --formatter org.apache.kafka.tools.consumer.DefaultMessageFormatter \
 * --property print.timestamp=true \
 * --property print.key=true \
 * --property print.value=true \
 * --property print.partition=true \
 * --bootstrap-server localhost:9092
 **/
public class ProducerWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());

    public static void main(String[] args) {

        log.info("producer : start");

        // config
        Properties props = new Properties();

        // config.localhost
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // create producer
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());
        props.setProperty("batch.size", "400"); // much too small for production - used to trigger parition behaviour

        // create msg
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Callback cb = new Callback() {
            @Override
            public void onCompletion(RecordMetadata meta, Exception e) {
                // executed onRecordSentSuccess or onException
                if (e == null) {
                    log.info("producer : callback : topic:{}, partition:{}, offset:{}, timestamp:{}", meta.topic(), meta.partition(), meta.offset(), meta.timestamp());
                } else {
                    log.error("producer : callback.error : {}", e.getMessage());
                }
            }
        };


        // batch send with delay between each
        for (int i = 0; i < 10; i++) {

            // send message
            for (int j = 0; j < 30; j++) {
                String value = "hello-world " + String.valueOf(i) + "." + String.valueOf(j);
                ProducerRecord<String, String> producerRecord = new ProducerRecord("demojava_p5", value);
                producer.send(producerRecord, cb);
            }

            // sleep
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


        // flush and close sync
        producer.flush();
        producer.close();
    }
}
