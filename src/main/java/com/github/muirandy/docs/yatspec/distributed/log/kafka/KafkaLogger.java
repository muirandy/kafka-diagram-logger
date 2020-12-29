package com.github.muirandy.docs.yatspec.distributed.log.kafka;

import com.github.muirandy.docs.living.api.DiagramLogger;
import com.github.muirandy.docs.living.api.Log;
import com.github.muirandy.docs.living.api.Logs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaLogger implements DiagramLogger {
    private static final String KAFKA_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String KAFKA_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String KAFKA_CONSUMER_GROUP = "kafka-diagram-logger";
    private static final String READ_TOPIC_FROM_BEGINNING = "earliest";
    private static final String SEQUENCE_DIAGRAM_END_MARKER = "SEQUENCE_DIAGRAM_END_MARKER";
    private static final String DEFAULT_KAFKA_TOPIC = "living-documentation";

    private final String kafkaHost;
    private final Integer kafkaPort;
    private final String topicName;

    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> kafkaProducer;

    public KafkaLogger(String kafkaHost, Integer kafkaPort) {
        this(kafkaHost, kafkaPort, DEFAULT_KAFKA_TOPIC);
    }

    public KafkaLogger(String kafkaHost, Integer kafkaPort, String topicName) {
        this.kafkaHost = kafkaHost;
        this.kafkaPort = kafkaPort;
        this.topicName = topicName;
    }

    @Override
    public void log(Log log) {
        sendMessageToKafkaTopic(log.getMessage(), log.getBody());
    }

    private void sendMessageToKafkaTopic(String key, String value) {
        try {
            KafkaProducer<String, String> kafkaProducer = getKafkaProducer();
            kafkaProducer.send(createProducerRecord(topicName, key, value)).get();
            kafkaProducer.flush();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private KafkaProducer<String, String> getKafkaProducer() {
        if (null == kafkaProducer)
            kafkaProducer = new KafkaProducer<>(kafkaPropertiesForProducer());
        return kafkaProducer;
    }

    private ProducerRecord createProducerRecord(String topicName, String key, String value) {
        return new ProducerRecord(topicName, key, value);
    }

    private Properties kafkaPropertiesForProducer() {
        Properties props = new Properties();
        props.put("acks", "all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER);
        return props;
    }

    private String getBootstrapServers() {
        return kafkaHost + ":" + kafkaPort;
    }

    @Override
    public Logs read() {
        Logs logs = new Logs();
        boolean keepReading = true;
        while (keepReading) {
            ConsumerRecords<String, String> kafkaRecords = readKafkaLogs();
            kafkaRecords.forEach(kr -> logs.add(new Log(kr.key(), kr.value())));
            if (kafkaRecords.isEmpty())
                keepReading = false;
        }
        return logs;
    }

    private ConsumerRecords<String, String> readKafkaLogs() {
        return getConsumer().poll(Duration.ofMillis(100));
    }

    private KafkaConsumer<String, String> getConsumer() {
        if (consumer == null)
            createKafkaConsumer();
        return consumer;
    }

    private void createKafkaConsumer() {
        consumer = new KafkaConsumer<>(kafkaPropertiesForConsumer());
        consumer.subscribe(List.of(topicName));
    }

    private Properties kafkaPropertiesForConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_CONSUMER_GROUP + Integer.toHexString(this.hashCode()));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_DESERIALIZER);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, READ_TOPIC_FROM_BEGINNING);
        return props;
    }

    @Override
    public void markEnd(String sequenceDiagramId) {
        sendMessageToKafkaTopic(SEQUENCE_DIAGRAM_END_MARKER, sequenceDiagramId);
    }

    @Override
    public Logs read(String logId) {

        return findRequestedLogs(logId);
    }

    private Logs findRequestedLogs(String logId) {
        boolean keepReading = true;
        Logs requestedLogs = new Logs();
        while (keepReading) {

            ConsumerRecords<String, String> consumerRecords = readKafkaLogs();

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                String k = consumerRecord.key();
                String v = consumerRecord.value();

                if (isEndMarker(k)) {
                    if (isRequestedEndMarker(v, logId))
                        return requestedLogs;
                    requestedLogs = new Logs();
                } else
                    requestedLogs.add(new Log(k, v));
            }
        }
        return requestedLogs;
    }

    private boolean isEndMarker(String k) {
        return (k.equals(SEQUENCE_DIAGRAM_END_MARKER));
    }

    private boolean isRequestedEndMarker(String v, String logId) {
        return v.equals(logId);
    }
}