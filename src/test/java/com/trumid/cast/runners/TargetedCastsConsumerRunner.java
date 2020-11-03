package com.trumid.cast.runners;

import com.trumid.cast.data.Cast;
import com.trumid.cast.data.CastKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.trumid.cast.kafka.config.KafkaProperties.castSubProperties;
import static com.trumid.cast.kafka.config.Topics.Casts;
import static com.trumid.cast.kafka.config.Topics.TargetedCasts;
import static java.lang.Runtime.getRuntime;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;

/** Just an example/test main to consume targeted and untargeted casts {@link org.apache.kafka.common.internals.Topic} */
public final class TargetedCastsConsumerRunner {
    private static final Logger log = LoggerFactory.getLogger(TargetedCastsConsumerRunner.class);
    private static final AtomicBoolean stayAlive = new AtomicBoolean(true);

    public static void main(String[] args) {
        log.info("Consuming targeted casts...");

        getRuntime().addShutdownHook(new Thread(() -> stayAlive.set(false)));

        try {
            final KafkaConsumer<CastKey, Cast> consumer = new KafkaConsumer<>(castSubProperties("on-my-own", 0));
            consumer.subscribe(asList(Casts.name(), TargetedCasts.name()));

            while (stayAlive.get()) {
                for (ConsumerRecord<CastKey, Cast> record : consumer.poll(ofMillis(100))) {
                    log.info("Received {} partition {} {}", record.topic(), record.partition(), record.value().toStringFull());
                }
            }

            log.info("Exiting");

        } catch (Exception e) {
            log.error("Failed to subscribe", e);
        }
    }
}
