package io.conduktor.demos.kafka;

import jdk.nashorn.internal.runtime.ECMAException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
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
        log.info("I am a Kafka Consumer");
        //create the producer properties
        String groupId = "my-java-application";
        String topic = "demo_java";
        Properties properties = new Properties();
        //connect to localhost
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        //connect to playground
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"6dLXzenSLc97OkuUYPGHD7\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2ZExYemVuU0xjOTdPa3VVWVBHSEQ3Iiwib3JnYW5pemF0aW9uSWQiOjczOTI1LCJ1c2VySWQiOjg1OTgyLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJmY2YxYzlhNy0wMWI3LTQ5YjYtOWY5NS0yM2E0N2I1NzVlMTQifX0.f37VGBLUikKC3IPh301oRSDrCHLtqlmiWXA7PySMLRs\";");
        properties.setProperty("sasl.mechanism","PLAIN");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());

        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.reset","earliest");

        //create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //get a reference to the main thread
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a shutdown, lets exit by calling consumer wakeup()");
                consumer.wakeup();

                //join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (Exception e){
                   e.printStackTrace();
                }
            }
        });

        try{
            consumer.subscribe(Arrays.asList(topic));
            while (true){
                log.info("polling");
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String,String> record:records){
                    log.info("key: "+record.key() +" Value: "+record.value());
                    log.info("Partition: "+record.partition() +" Offset: "+record.offset());
                }
            }
        }catch (WakeupException e){
            log.info("consumer is starting to shutdown");
        } catch (Exception e){
            log.info("unexpted exception in the consumer",e);
        } finally {
            consumer.close();
            log.info("the consumer os now gracefully shutdown");
        }
        //subscribe to a topic

    }
}