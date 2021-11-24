package com.sayeeda.kbc.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
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
        producer.send(record);

        //producer.flush();
        producer.close(); //Used because producer.send() is async and this makes sure the event is sent to topic
    }
}
