package org.atif.com.demos.produce;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Salam Alaykum...!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        //create Consumer properties
        Properties properties = new Properties();

        //localhost connection
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set Consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        //none - if do not have consumer group, so we must set consumer group else application will fail
        //earliest - mean read from beginning of the topic
        //latest - read from now on (only the new messages)
//        properties.setProperty("auto.offset.reset","none/earliest/latest");
        properties.setProperty("auto.offset.reset", "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to topic
//        consumer.subscribe(Arrays.asList("topic1","topic2","topic3"));
        consumer.subscribe(Arrays.asList(topic));

        //poll for data
        while (true) {
            log.info("data polling...");

            //how long we are willing to receive data
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info("key: " + record.key() + " | value: " + record.value());
                log.info("partition: " + record.partition() + " | offset: " + record.offset());
            }
        }
    }
}
