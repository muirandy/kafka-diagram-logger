package com.github.muirandy.docs.yatspec.distributed.log.kafka;

import com.github.muirandy.docs.yatspec.distributed.DiagramLogger;
import com.github.muirandy.docs.yatspec.distributed.Logs;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaLoggerShould {

    private static final String TOPIC_NAME = "living-documentation";

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))
                ;

    
    @Test
    void returnEmptyLogs() {
        String kafkaHost = kafka.getHost();
        Integer kafkaPort = kafka.getMappedPort(9092);

        DiagramLogger kafkaLogger = new KafkaLogger(kafkaHost, kafkaPort, TOPIC_NAME);
        Logs logs = kafkaLogger.read();

        assertThat(logs.getLogs()).isEmpty();
    }
}
