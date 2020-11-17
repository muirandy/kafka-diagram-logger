package com.github.muirandy.docs.yatspec.distributed.log.kafka;

import com.github.muirandy.docs.yatspec.distributed.DiagramLogger;
import com.github.muirandy.docs.yatspec.distributed.Log;
import com.github.muirandy.docs.yatspec.distributed.Logs;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaLoggerShould {

    private static final String TOPIC_NAME = "living-documentation";
    private static final String LOG_MESSAGE = "Message";
    private static final Object BODY = "Body";

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))
                ;

    DiagramLogger kafkaLogger;
    private Log log = new Log(LOG_MESSAGE, BODY);

    @BeforeEach
    void setUp() {
        String kafkaHost = kafka.getHost();
        Integer kafkaPort = kafka.getMappedPort(9092);
        kafkaLogger = new KafkaLogger(kafkaHost, kafkaPort, TOPIC_NAME);
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
}
