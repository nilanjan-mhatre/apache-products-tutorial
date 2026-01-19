package com.sample.apache.kafka;

import org.apache.kafka.clients.producer.*;

public class SampleKafkaProducer {

    private static final String TOPIC = "test-topic";

    public static void main(String[] args) {
        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(KafkaConfig.producerProps())) {

            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, "key-" + i, "message-" + i);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.printf(
                                "Sent to topic=%s partition=%d offset=%d%n",
                                metadata.topic(), metadata.partition(), metadata.offset()
                        );
                    }
                });
            }

            producer.flush();
        }
    }
}
