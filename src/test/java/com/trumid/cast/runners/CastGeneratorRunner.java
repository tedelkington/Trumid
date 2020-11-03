package com.trumid.cast.runners;

import com.trumid.cast.data.Cast;
import com.trumid.cast.data.CastKey;
import com.trumid.cast.kafka.config.CastPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.trumid.cast.contract.Side.Buy;
import static com.trumid.cast.contract.Side.Sell;
import static com.trumid.cast.kafka.config.KafkaProperties.initialCastStreamProperties;
import static com.trumid.cast.kafka.config.Topics.Casts;
import static java.lang.Runtime.getRuntime;
import static java.lang.Thread.sleep;

/**
 * The initial starting stream of casts, outside our services
 *
 * Partitions by {@link CastPartitioner}
 **/
public final class CastGeneratorRunner {
    private static final Logger log = LoggerFactory.getLogger(CastGeneratorRunner.class);
    private static final AtomicBoolean stayAlive = new AtomicBoolean(true);

    public static void main(String[] args) {
        log.info("Sending casts...");

        getRuntime().addShutdownHook(new Thread(() -> stayAlive.set(false)));

        try {
            final Producer<CastKey, Cast> producer = new KafkaProducer<>(initialCastStreamProperties());
            int seed = 0;

            while (stayAlive.get()) {
                final Cast cast = generateCast(seed++);
                producer.send(new ProducerRecord<>(Casts.name(), cast.key, cast));
                log.info("Generating {}", cast.toStringFull());
                sleep(1_200);
            }

            producer.close();

        } catch (Exception e) {
            log.error("Failed to publish", e);
        }
    }

    /** generate a cast switching sides, originators and incrementing/decrementing prices and qtys */
    private static Cast generateCast(int seed) {
        final int numberOfOriginators = 5;
        final int numberOfInstruments = 10;
        final boolean isBuy = seed % 2 == 0;
        final int adjustment = seed % 20;

        if (isBuy) {
            return new Cast(seed % numberOfOriginators, seed % numberOfInstruments, Buy)
                    .price(99 - adjustment)
                    .quantity(1000 - adjustment)
                    .targetUserIds(1, 2);
        } else {
            return new Cast(seed % numberOfOriginators, seed % numberOfInstruments, Sell)
                    .price(100 + adjustment)
                    .quantity(2000 + adjustment)
                    .targetUserIds(1, 3);
        }
    }
}
