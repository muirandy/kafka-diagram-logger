package com.github.muirandy.docs.yatspec.distributed.log.kafka;

import com.github.muirandy.docs.yatspec.distributed.DiagramLogger;
import com.github.muirandy.docs.yatspec.distributed.Log;
import com.github.muirandy.docs.yatspec.distributed.Logs;

public class KafkaLogger implements DiagramLogger {
    public KafkaLogger(String kafkaHost, Integer kafkaPort, String topicName) {
    }

    @Override
    public void log(Log log) {

    }

    @Override
    public Logs read() {
        return new Logs();
    }
}
