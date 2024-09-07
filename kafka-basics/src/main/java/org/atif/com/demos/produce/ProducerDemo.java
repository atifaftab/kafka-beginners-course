package org.atif.com.demos.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        System.out.println("Salam Alaykum...!");
        log.info("Salam Alaykum...!");

        //create producer properties

        Properties properties = new Properties();
//        properties.setProperty("key", "value");
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Good good");

        //send data
        producer.send(producerRecord); //.send() - it runs asynchronously unless we use flush or close.

        //tell the producer to send all data and block until done --synchronous
        producer.flush();

        //flush and close the producer
        producer.close();  // .close() - flush and close, but just to highlight we can use flush  separately
    }
}
