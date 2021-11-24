package com.sayeeda.kbc.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

//This class illustrates how we can associate keys with values (message values) so that messages on one key will always go to same partition.
public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        String bootstrapServers = "localhost:9092";

        //Step 1: Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Step 2: Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Step 3: Create a producer record to send to topic
        for(int i = 0; i < 10; i++) {
            String topicName = "myFirstTopic";
            String valueInMsg = "Hello " + i;
            String key = "id_" + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, valueInMsg);

            //Step 4: Send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Record received. \n" +
                                "Key: " + key + "\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing event : ", e);
                    }
                }
            }).get(); //Added .get() to send() function to block further sends and make it synchronous. Not recommended in production
        }

        //producer.flush();
        producer.close(); //Used because producer.send() is async and this makes sure the event is sent to topic
    }
}