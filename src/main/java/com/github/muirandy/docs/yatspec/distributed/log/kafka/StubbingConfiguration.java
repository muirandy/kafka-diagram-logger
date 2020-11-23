package com.github.muirandy.docs.yatspec.distributed.log.kafka;

public class StubbingConfiguration {
    public String sourceSystemName;
    public String thisSystemName;

    public StubbingConfiguration() {
    }

    public StubbingConfiguration(String sourceSystemName, String thisSystemName) {
        this.sourceSystemName = sourceSystemName;
        this.thisSystemName = thisSystemName;
    }
}
