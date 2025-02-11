package com.multimonos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.DocFlavor;
import java.util.Properties;

public class ProducerWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeys.class.getSimpleName());

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

        // goal
        // - the key defines which parition the msg is written to
        // - so, for each key for each batch the partition is identical

        // send msg with same key "i" times
        for (int i = 0; i < 3; i++) {

            // send message "j"
            for (int j = 0; j < 5; j++) {
                String topic = "demojava_p5";
                String key = "id_" + String.valueOf(j);
                String value = "hello-world " + String.valueOf(j);

                ProducerRecord<String, String> producerRecord = new ProducerRecord(topic, key, value);

                Callback callback = new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata meta, Exception e) {
                        // executed onRecordSentSuccess or onException
                        if (e == null) {
                            log.info("producer : callback : key:{}, partition:{}, offset:{}", key, meta.partition(), meta.offset());
                        } else {
                            log.error("producer : callback.error : {}", e.getMessage());
                        }
                    }
                };

                producer.send(producerRecord, callback);
            }

            // sleep
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }


        // flush and close sync
        producer.flush();
        producer.close();
    }
}
