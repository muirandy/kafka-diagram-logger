package com.github.muirandy.docs.yatspec.distributed.log.kafka;

import java.util.Optional;

public class KafkaConfiguration extends StubbingConfiguration {
    public String kafkaHost;
    public int kafkaPort;
    public Optional<String> kafkaTopic;

    public KafkaConfiguration() {
        super();
    }

    public KafkaConfiguration(String sourceSystemName, String thisSystemName, String kafkaHost, int kafkaPort, String kafkaTopic) {
        super(sourceSystemName, thisSystemName);
        this.kafkaHost = kafkaHost;
        this.kafkaPort = kafkaPort;
        this.kafkaTopic = Optional.of(kafkaTopic);
    }

    public KafkaConfiguration(String sourceSystemName, String thisSystemName, String kafkaHost, int kafkaPort) {
        super(sourceSystemName, thisSystemName);
        this.kafkaHost = kafkaHost;
        this.kafkaPort = kafkaPort;
        this.kafkaTopic = Optional.empty();
    }
}
