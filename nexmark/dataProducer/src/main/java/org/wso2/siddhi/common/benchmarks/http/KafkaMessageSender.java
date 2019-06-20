package org.wso2.siddhi.common.benchmarks.http;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * The class for sending messages to Kafka.
 */
public class KafkaMessageSender {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC1 = "test15";
    private static final String TOPIC2 = "test16";
    private static final String TOPIC3 = "test17";
    private static final String TOPIC4 = "test18";
    private static final Logger log = Logger.getLogger(KafkaMessageSender.class);

    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void runProducer1(String jSON,Producer producer) throws Exception {

        //final Producer<String, String> producer = createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC1, jSON);

        try {
            producer.send(record, null);
            producer.flush();
            //log.info("Message sent to kafka topic of " + TOPIC);
        } finally {
            producer.flush();
            //producer.close();
        }
    }

    public static void runProducer2(String jSON,Producer producer) throws Exception {

        //final Producer<String, String> producer = createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC2, jSON);

        try {
            producer.send(record, null);
            producer.flush();
            //log.info("Message sent to kafka topic of " + TOPIC);
        } finally {
            producer.flush();
            //producer.close();
        }
    }

    public static void runProducer3(String jSON,Producer producer) throws Exception {

        //final Producer<String, String> producer = createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC3, jSON);

        try {
            producer.send(record, null);
            producer.flush();
            //log.info("Message sent to kafka topic of " + TOPIC);
        } finally {
            producer.flush();
            //producer.close();
        }
    }
    public static void runProducer4(String jSON,Producer producer) throws Exception {

        //final Producer<String, String> producer = createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC4, jSON);

        try {
            producer.send(record, null);
            producer.flush();
            //log.info("Message sent to kafka topic of " + TOPIC);
        } finally {
            producer.flush();
            //producer.close();
        }
    }
}
