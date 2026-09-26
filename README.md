# Kafka Java Examples

A learning repository for the Apache Kafka Java client. The examples use the `io.conduktor.demos.kafka` package.

## Examples

| Class | Focus |
| --- | --- |
| `ProducerDemo` | Sending records |
| `ProducerDemoCallBack` | Handling send callbacks |
| `ProducerDemoKeys` | Sending records with keys |
| `ConsumerDemo` | Polling records, consumer groups, and graceful shutdown |

## Stack

Java 8 target, Maven, Kafka Clients 3.0.0, and SLF4J.

## Getting oriented

1. Import `pom.xml` into your Java IDE.
2. Review the broker and authentication settings in each example before running it.
3. Configure a Kafka cluster you control and a `demo_java` topic.
4. Run the producer or consumer class from your IDE.

This is an educational snapshot. It includes legacy configuration and is not a production service. Use your own connection configuration; never commit credentials.
