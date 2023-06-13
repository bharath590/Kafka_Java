package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");
        //create the producer properties

        Properties properties = new Properties();
        //connect to localhost
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //connect to playground
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"6dLXzenSLc97OkuUYPGHD7\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2ZExYemVuU0xjOTdPa3VVWVBHSEQ3Iiwib3JnYW5pemF0aW9uSWQiOjczOTI1LCJ1c2VySWQiOjg1OTgyLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJmY2YxYzlhNy0wMWI3LTQ5YjYtOWY5NS0yM2E0N2I1NzVlMTQifX0.f37VGBLUikKC3IPh301oRSDrCHLtqlmiWXA7PySMLRs\";");
        properties.setProperty("sasl.mechanism","PLAIN");

        //create the producer
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        //send data
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //create a Producer Record
        //send the data
        for(int i=0;i<2;i++) {
            for (int j = 0; j < 10; j++) {
                String key = "id_" + j;
                String val = "hello world " + j;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", key, val);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //execute everytime a record succesfully send or an exception
                        log.info("Received new meta data \n" +
                                "Key " + key + " | " + "Partition: " + recordMetadata.partition() + " \n"
                        );

                    }
                });
            }
            try{
                Thread.sleep(500);
            }catch (Exception e){

            }
        }
        //flush and close producer
        producer.flush();
        producer.close();
    }
}