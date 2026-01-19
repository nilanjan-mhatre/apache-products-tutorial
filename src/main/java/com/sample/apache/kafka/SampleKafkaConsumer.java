package com.sample.apache.kafka;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Collections;


/*

kafka-storage.bat format --standalone -t kafkastore -c C:\Apache\kafka_2.13-4.1.1\config\server.properties

kafka-topics --create  --topic test-topic  --bootstrap-server localhost:9092  --partitions 3  --replication-factor 1

kafka-server-start C:\Apache\kafka_2.13-4.1.1\config\server.properties

kafka-storage.bat format --standalone -t kafkastore -c C:\Apache\kafka_2.13-4.1.1\config\server.properties
*/

public class SampleKafkaConsumer {
    private static final String TOPIC = "test-topic";

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(KafkaConfig.consumerProps("simple-consumer-group"));

        consumer.subscribe(Collections.singletonList(TOPIC));

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf(
                        "Consumed key=%s value=%s partition=%d offset=%d%n",
                        record.key(), record.value(),
                        record.partition(), record.offset()
                );
            }
        }
    }
}
