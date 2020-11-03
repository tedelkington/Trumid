package com.trumid.cast.runners;

import com.trumid.cast.kafka.actioners.TargetedCastMicroService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TargetedCastMicroServiceRunner {
    private static final Logger log = LoggerFactory.getLogger(TargetedCastMicroServiceRunner.class);

    public static void main(String[] args) {
        log.info("Starting targeted cast micro service...");

        new TargetedCastMicroService().start();

        while (true) {

        }
    }
}
