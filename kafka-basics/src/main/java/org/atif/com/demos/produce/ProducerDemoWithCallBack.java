package org.atif.com.demos.produce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());

    public static void main(String[] args) {
        log.info("ProducerDemoWithCallBack starts...");

        //create producer properties

        Properties properties = new Properties();
//        properties.setProperty("key", "value");
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //we want to produce kafka messages 10 times
        for (int i = 0; i < 10; i++) {
            //create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello there " + i);

            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //execute everytime when a record successfully sent an exception is thrown
                    if (exception != null) {
                        log.error("Error while producing", exception);
                    } else {
                        //the record sent successfully
                        log.info("Received new metadata \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp()
                        );
                    }
                }
            }); //.send() - it runs asynchronously unless we use flush or close.
        }

        //tell the producer to send all data and block until done --synchronous
        producer.flush();

        //flush and close the producer
        producer.close();  // .close() - flush and close, but just to highlight we can use flush  separately
    }
}
