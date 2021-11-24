package com.sayeeda.kbc.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCalback {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCalback.class);

        String bootstrapServers = "localhost:9092";

        //Step 1: Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Step 2: Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Step 3: Create a producer record to send to topic
        ProducerRecord<String, String> record = new ProducerRecord<>("myFirstTopic", "Hello world!");

        //Step 4: Send data
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    logger.info("Record received. \n"+
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                }
                else{
                    logger.error("Error while producing event : ", e);
                }
            }
        });

        //producer.flush();
        producer.close(); //Used because producer.send() is async and this makes sure the event is sent to topic
    }
}