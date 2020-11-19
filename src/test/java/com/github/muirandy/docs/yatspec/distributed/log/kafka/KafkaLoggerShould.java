package com.github.muirandy.docs.yatspec.distributed.log.kafka;

import com.github.muirandy.docs.yatspec.distributed.DiagramLogger;
import com.github.muirandy.docs.yatspec.distributed.Log;
import com.github.muirandy.docs.yatspec.distributed.Logs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaLoggerShould {

    private static final String KAFKA_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String TOPIC_NAME = "living-documentation";
    private static final String LOG_MESSAGE = "Message";
    private static final String SECOND_LOG_MESSAGE = "Second Message";
    private static final String BODY = "Body";
    private static final String SECOND_BODY = "Second Body";
    private static final int WORKING_KAFKA_BROKER_PORT = 9093;


    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))
                ;

    DiagramLogger kafkaLogger;
    private Log log = new Log(LOG_MESSAGE, BODY);
    private Log secondLog = new Log(SECOND_LOG_MESSAGE, SECOND_BODY);
    private String kafkaHost;
    private Integer kafkaPort;
    private String logId = UUID.randomUUID().toString();

    @BeforeEach
    void setUp() {
        kafkaLogger = createKafkaLogger();
    }

    private KafkaLogger createKafkaLogger() {
        kafkaHost = kafka.getHost();
        kafkaPort = kafka.getMappedPort(WORKING_KAFKA_BROKER_PORT);
        return new KafkaLogger(kafkaHost, kafkaPort, TOPIC_NAME);
    }

    @Test
    void returnEmptyLogs() {
        Logs logs = kafkaLogger.read();

        assertThat(logs.getLogs()).isEmpty();
    }

    @Test
    void retrieveLogs() {
        kafkaLogger.log(log);

        Logs logs = kafkaLogger.read();

        assertThat(logs.getLogs()).containsExactly(log);
    }

    @Test
    void retrieveLogsWrittenByAnotherProcess() {
        writeLogToKafkaIndependently();

        Logs logs = kafkaLogger.read();

        assertThat(logs.getLogs()).containsExactly(log);
    }

    @Test
    void shareLogsWithOtherProcesses() {
        kafkaLogger.log(log);

        Logs logs = createKafkaLogger().read();

        assertThat(logs.getLogs()).containsExactly(log);
    }

    @Test
    void retrieveEmptyLogsForGivenId() {
        kafkaLogger.markEnd(logId);

        Logs logs = kafkaLogger.read(logId);

        assertThat(logs.getLogs()).isEmpty();
    }

    @Test
    void retrieveSingleLogForGivenId() {
        kafkaLogger.log(log);
        kafkaLogger.markEnd(logId);

        Logs logs = kafkaLogger.read(logId);

        assertThat(logs.getLogs()).containsExactly(log);
    }

    @Test
    void excludePriorLogs() {
        kafkaLogger.log(log);
        kafkaLogger.markEnd("Previous Id");
        kafkaLogger.log(secondLog);
        kafkaLogger.markEnd(logId);

        Logs logs = kafkaLogger.read(logId);

        assertThat(logs.getLogs()).containsExactly(secondLog);
    }

    private void writeLogToKafkaIndependently() {
        sendMessageToKafkaTopic(log.getMessage(), log.getBody());
    }

    private void sendMessageToKafkaTopic(String key, String value) {
        try {
            getStringStringKafkaProducer().send(createProducerRecord(TOPIC_NAME, key, value)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private KafkaProducer<String, String> getStringStringKafkaProducer() {
        return new KafkaProducer<>(kafkaPropertiesForProducer());
    }

    private Properties kafkaPropertiesForProducer() {
        Properties props = new Properties();
        props.put("acks", "all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getExternalBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZER);
        return props;
    }

    private String getExternalBootstrapServers() {
        return "localhost:" + kafkaPort;
    }

    private ProducerRecord createProducerRecord(String topicName, String key, String value) {
        return new ProducerRecord(topicName, key, value);
    }

}
