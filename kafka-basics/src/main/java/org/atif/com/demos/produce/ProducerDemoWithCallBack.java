package org.atif.com.demos.produce;

import org.apache.kafka.clients.producer.*;
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

        //DO NOT DO this in PRODUCTION
        //just for understanding partitions

        //setting batch size to 400, so we can change partitioner after size finish
        //but by default it is 16KB
//        properties.setProperty("batch.size", "400");

        //setting partitioner to round-robin
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());


        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //
        for (int j = 0; j < 10; j++) {
            //we want to produce kafka messages for batch of 30
            for (int i = 0; i < 30; i++) {
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
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //tell the producer to send all data and block until done --synchronous
        producer.flush();

        //flush and close the producer
        producer.close();  // .close() - flush and close, but just to highlight we can use flush  separately
    }
}
