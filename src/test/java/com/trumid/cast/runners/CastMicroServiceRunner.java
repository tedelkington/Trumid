package com.trumid.cast.runners;

import com.trumid.cast.kafka.actioners.CastMicroService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CastMicroServiceRunner {
    private static final Logger log = LoggerFactory.getLogger(CastMicroServiceRunner.class);

    public static void main(String[] args) {
        log.info("Starting cast micro service...");

        new CastMicroService().start();

        while (true) {

        }
    }
}
