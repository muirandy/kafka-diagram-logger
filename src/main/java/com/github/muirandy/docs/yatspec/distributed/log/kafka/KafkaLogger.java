package com.github.muirandy.docs.yatspec.distributed.log.kafka;

import com.github.muirandy.docs.yatspec.distributed.DiagramLogger;
import com.github.muirandy.docs.yatspec.distributed.Log;
import com.github.muirandy.docs.yatspec.distributed.Logs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaLogger implements DiagramLogger {
    private static final String KAFKA_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String KAFKA_CONSUMER_GROUP = "kafka-diagram-logger";
    private static final String READ_TOPIC_FROM_BEGINNING = "earliest";

    private final String kafkaHost;
    private final Integer kafkaPort;
    private final String topicName;

    private Logs logs = new Logs();

    public KafkaLogger(String kafkaHost, Integer kafkaPort, String topicName) {
        this.kafkaHost = kafkaHost;
        this.kafkaPort = kafkaPort;
        this.topicName = topicName;
    }

    @Override
    public void log(Log log) {
        logs.add(log);
    }

    @Override
    public Logs read() {
        ConsumerRecords<String,String> kafkaRecords = readKafkaLogs();
        kafkaRecords.forEach(kr -> logs.add(new Log(kr.key(), kr.value())));
        return logs;
    }

    private ConsumerRecords<String, String> readKafkaLogs() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer
                <>(kafkaPropertiesForConsumer());
        consumer.subscribe(List.of(topicName));

        return consumer.poll(Duration.ofMillis(500));
    }

    private Properties kafkaPropertiesForConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getExternalBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, READ_TOPIC_FROM_BEGINNING);
        return props;
    }

    private String getExternalBootstrapServers() {
        return kafkaHost + ":" + kafkaPort;
    }
}
