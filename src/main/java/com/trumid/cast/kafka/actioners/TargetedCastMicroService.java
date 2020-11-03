package com.trumid.cast.kafka.actioners;

import com.trumid.cast.data.Cast;
import com.trumid.cast.data.CastKey;
import com.trumid.cast.kafka.config.InstanceGroup;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.trumid.cast.kafka.config.KafkaProperties.activeSubProperties;
import static com.trumid.cast.kafka.config.Topics.ActiveCasts;
import static java.util.Arrays.asList;

/**
 * These consume {@link com.trumid.cast.kafka.config.Topics#TargetedCasts} and proof that we have
 * a dedicated stream for each target (
 */
public final class TargetedCastMicroService { // TODO delete
    private final Logger log = LoggerFactory.getLogger(TargetedCastMicroService.class);
    private final AtomicBoolean stayAlive = new AtomicBoolean(true);
    private final InstanceGroup instanceGroup = new InstanceGroup();
//    private final Consumer<In, Cast> casts;
    private final Consumer<Integer, Integer> active;

    public TargetedCastMicroService() {
        active = new KafkaConsumer<>(activeSubProperties("SelectConsumer-" + instanceGroup.instanceId));
    }

    public void start() {
        log.info("Consuming targeted casts and getActiveCast requests {}", instanceGroup);

        active.subscribe(asList(ActiveCasts.name()));
    }
}
