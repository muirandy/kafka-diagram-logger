package com.github.muirandy.docs.yatspec.distributed.log.kafka;

import com.github.muirandy.docs.yatspec.distributed.DiagramLogger;
import com.github.muirandy.docs.yatspec.distributed.Log;
import com.github.muirandy.docs.yatspec.distributed.Logs;

public class KafkaLogger implements DiagramLogger {
    private Logs logs = new Logs();

    public KafkaLogger(String kafkaHost, Integer kafkaPort, String topicName) {
    }

    @Override
    public void log(Log log) {
        logs.add(log);
    }

    @Override
    public Logs read() {
        return logs;
    }
}
