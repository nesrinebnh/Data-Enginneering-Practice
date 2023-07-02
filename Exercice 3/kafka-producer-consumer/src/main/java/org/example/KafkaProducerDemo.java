package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Scanner;

public class KafkaProducerDemo {
    public static void main(String[] args){
        Scanner sc = new Scanner(System.in);
        Logger logger = LoggerFactory.getLogger(KafkaProducerDemo.class);


        String bootstrapName="127.0.0.1:9092";

        //Create producer properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapName);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(props);

        while(true){
            System.out.println("Give a message to send");
            String message = sc.nextLine();

            //create producer record
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("test", message);

            //send data
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        logger.info("received new metadata:\n Topic: "+recordMetadata.topic()+"\n"
                                +" Partition: "+recordMetadata.partition()+"\n"
                                +" Offset: "+recordMetadata.offset()+"\n"
                                +" Timestamp: "+recordMetadata.timestamp());
                    }else{
                        logger.error("Error while producing "+e);
                    }

                }
            });
            kafkaProducer.close();
        }

    }


}
