package com.trumid.cast.kafka.actioners;

import com.trumid.cast.data.Cast;
import com.trumid.cast.data.CastKey;
import com.trumid.cast.kafka.config.InstanceGroup;
import com.trumid.cast.kafka.serialize.CastKeySerde;
import com.trumid.cast.kafka.serialize.CastSerde;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.trumid.cast.kafka.config.KafkaProperties.*;
import static com.trumid.cast.kafka.config.Topics.*;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;
import static org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore;

/**
 * These consume {@link com.trumid.cast.kafka.config.Topics#TargetedCasts}
 */
public final class TargetedCastMicroService {
    private final Logger log = LoggerFactory.getLogger(TargetedCastMicroService.class);
    private final AtomicBoolean stayAlive = new AtomicBoolean(true);
    private final InstanceGroup instanceGroup = new InstanceGroup();
    private final Consumer<CastKey, Cast> targetedCasts;
    private final Producer<Integer, String> replies;
    private final Consumer<Integer, Integer> active;
    private final Properties targetedProps;

    public TargetedCastMicroService() {
        targetedProps = targetedCastSubProperties(TargetedCastMicroService.class.getSimpleName(), instanceGroup.instanceId);
        targetedCasts = new KafkaConsumer<>(targetedProps);
        replies = new KafkaProducer<>(replyPubProperties());
        active = new KafkaConsumer<>(activeSubProperties("SelectConsumer-" + instanceGroup.instanceId));
    }

    public void start() {
        log.info("Consuming targeted casts and getActiveCast requests {}", instanceGroup);

        targetedCasts.subscribe(asList(TargetedCasts.name()));
        active.subscribe(asList(ActiveCasts.name()));

        while (stayAlive.get()) {
            targetedCasts.poll(ofMillis(100)).forEach(record -> {
                log.info("Received {} on partition {} {}", record.topic(), record.partition(), record.value().toStringFull());
            });

            // OK - so kinda options (perhaps more) - these consumers can't consumer by explicit partition
            active.poll(ofMillis(100)).forEach(record -> {

                final Integer targetUserId = record.key();
                final Integer requestId = record.value();

                if (instanceGroup.isMine(targetUserId)) {

                    final StreamsBuilder builder = new StreamsBuilder();
                    builder.globalTable(
                            Casts.name(),
                            Materialized.<CastKey, Cast, KeyValueStore<Bytes, byte[]>>as("casts-global-store")
                                    .withKeySerde(new CastKeySerde())
                                    .withValueSerde(new CastSerde())
                    );

                    final KafkaStreams streams = new KafkaStreams(builder.build(), castSubProperties(CastMicroService.class.getSimpleName(), instanceGroup.instanceId));
                    streams.start();

                    final ReadOnlyKeyValueStore<CastKey, Cast> store = streams.store(fromNameAndType("casts-global-store", keyValueStore()));
                    final Map<CastKey, Cast> results = new HashMap<>(); // after all that we're back to a map ; )
                    store.all().forEachRemaining(kv -> {
                        if (kv.value.isForTargetUserId(targetUserId)) {
                            log.info("Matched {} {}", kv.key, kv.value.toStringFull());
                            results.put(kv.key, kv.value);
                        }
                    });

                    streams.close();

                    log.info("Sending success reply for {} {}", requestId, results);
                    replies.send(new ProducerRecord<>(Reply.name(), requestId, results.size() + " active"));

                    // standard, local KTable - which obv only gets my (currently random) partition(s)
//                    final StreamsBuilder builder = new StreamsBuilder();
//                    final KStream<CastKey, Cast> stream = builder.stream(Casts.name());
//                    final KTable<CastKey, ActiveCast> activeCasts = stream
//                            .filter((castKey, cast) -> cast.isForTargetUserId(targetUserId))
//                            .filter((castKey, cast) -> Active == cast.status())
//                            .groupByKey().aggregate(
//                                    () -> new ActiveCast(),
//                                    (castKey, cast, aggregate) -> {
//                                        log.info("Aggregating {} {}", castKey, cast.toStringFull());
//                                        aggregate.activeCast(cast);
//                                        return aggregate; },
//                                    Materialized.with(new CastKeySerde(), new ActiveCastSerde()));
//
//                    final KafkaStreams streams = new KafkaStreams(builder.build(), targetedProps);
//                    streams.start();
//
//                    final AtomicInteger count = new AtomicInteger(0); // I guess you should never do this...violating the approach
//                    activeCasts.toStream().foreach((k, c) -> {
//                            log.info("targetUserId={} has active cast {}", targetUserId, c.activeCast().toStringFull());
//                            count.incrementAndGet(); });
//
//                    streams.close();

//                    log.info("Sending success reply for " + requestId);
//                    replies.send(new ProducerRecord<>(Reply.name(), requestId, count.get() + " active"));

                } else {
                    log.info("Skipping select for targetUserId {}", targetUserId);
                }
            });
        }

        targetedCasts.close();
        replies.close();
        active.close();
    }
}
