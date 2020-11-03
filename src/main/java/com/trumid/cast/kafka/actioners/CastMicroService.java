package com.trumid.cast.kafka.actioners;

import com.trumid.cast.data.Cast;
import com.trumid.cast.data.CastKey;
import com.trumid.cast.kafka.config.CastPartitioner;
import com.trumid.cast.kafka.config.InstanceGroup;
import com.trumid.cast.kafka.events.CommandEvent;
import com.trumid.cast.kafka.serialize.CastKeySerde;
import com.trumid.cast.kafka.serialize.CastSerde;
import com.trumid.cast.kafka.serialize.ActiveCastSerde;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.trumid.cast.contract.CastStatus.Active;
import static com.trumid.cast.contract.CastStatus.Canceled;
import static com.trumid.cast.kafka.config.Command.Activate;
import static com.trumid.cast.kafka.config.KafkaProperties.*;
import static com.trumid.cast.kafka.config.Topics.*;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

/**
 * Consumes a partition from {@link com.trumid.cast.kafka.config.Topics#Casts} and marks those as
 * {@link com.trumid.cast.contract.CastStatus#Active} (assuming they have been marked as active via request)
 * and publishes to {@link com.trumid.cast.kafka.config.Topics#TargetedCasts}
 *
 * The targeted cast topic is </Integer, Cast> so depending on number of targets, we could even consider having a targeted
 * topic per target - or we can stripe by targetedId % numberOfConsumers
 */
public final class CastMicroService {
    private final Logger log = LoggerFactory.getLogger(CastMicroService.class);
    private final AtomicBoolean stayAlive = new AtomicBoolean(true);
    private final InstanceGroup instanceGroup = new InstanceGroup();
    private final Map<CastKey, Cast> cache = new HashMap<>();
    private final CastPartitioner partitioner = new CastPartitioner();
    private final Producer<Integer, Cast> targetedCasts = new KafkaProducer<>(targetedCastPubProperties());
    private final Consumer<CastKey, Cast> casts;
    private final Consumer<Integer, CommandEvent> commands;
    private final Producer<Integer, String> replies;
    private final Consumer<Integer, Integer> active;
//    private final GlobalKTable<CastKey, Cast> globalCasts;

    public CastMicroService() {
        casts = new KafkaConsumer<>(castSubProperties(CastMicroService.class.getSimpleName(), instanceGroup.instanceId));
        commands = new KafkaConsumer<>(commandSubProperties("CommandConsumer-" + instanceGroup.instanceId));
        replies = new KafkaProducer<>(replyPubProperties());
        active = new KafkaConsumer<>(activeSubProperties("SelectConsumer-" + instanceGroup.instanceId));

//        final StreamsBuilder builder = new StreamsBuilder();
//        globalCasts = builder.globalTable(
//                Casts.name(),
//                Materialized.<CastKey, Cast, KeyValueStore<Bytes, byte[]>>as("casts-global-store")
//                        .withKeySerde(new CastKeySerde())
//                        .withValueSerde(new CastSerde())
//        );
//
//        final KafkaStreams streams = new KafkaStreams(builder.build(), castSubProperties(CastMicroService.class.getSimpleName(), instanceGroup.instanceId));
//        streams.start();
    }

    public void start() {
        log.info("Consuming casts and commands {}", instanceGroup);

        // I've gone for explicit partition specification, rather than pattern sub - on the basis failover (when added) would be simpler
        casts.assign(asList(new TopicPartition(Casts.name(), instanceGroup.instanceId)));
        commands.subscribe(asList(Commands.name()));
        active.subscribe(asList(ActiveCasts.name()));

        while (stayAlive.get()) {
            casts.poll(ofMillis(100)).forEach(record -> {
                final CastKey key = record.key();
                final Cast cast = record.value();
                cache.put(key, cast);
                log.info("Received {} on partition {} {}", record.topic(), record.partition(), cast.toStringFull());

                if (isAlreadyActive(key)) {
                    sendTargetedCast(cast);
                }
            });

            commands.poll(ofMillis(100)).forEach(record -> {
                final Integer requestId = record.key();
                final CommandEvent event = record.value();
                if (! isCommandForMyOriginators(event)) {
                    log.info("Skipping {} for {}", event, requestId);
                    return;
                }

                log.info("Processing {} for {}", event, requestId);

                switch (event.command) {
                    case Activate:
                        final Cast activeCast = cache.computeIfPresent(event.key, (k, cast) -> {
                            cast.status(Active);
                            sendTargetedCast(cast);
                            return cast;
                        });

                        if (null != activeCast) {
                            replySuccess(requestId);
                        } else {
                            replyError(requestId, Activate.name() + " received for non-existent cast");
                        }
                        break;

                    case Cancel:
                        final Cast canceledCast = cache.computeIfPresent(event.key, (k, cast) -> {
                            cast.status(Canceled);
                            sendTargetedCast(cast);
                            return cast;
                        });

                        if (null != canceledCast) {
                            replySuccess(requestId);
                        } else {
                            replyError(requestId, Canceled.name() + " received for non-existent cast");
                        }
                        break;

                    default:
                        replyError(requestId, "Unhandled command received " + event + " for request " + requestId);
                }
            });

            active.poll(ofMillis(100)).forEach(record -> {

                final Integer targetUserId = record.key();
                if (instanceGroup.isMine(targetUserId)) {

                    final StreamsBuilder builder = new StreamsBuilder();
                    final KStream<CastKey, Cast> stream = builder.stream(Casts.name());
                    final KTable<CastKey, ActiveCast> activeCasts = stream
                            .filter((castKey, cast) -> cast.isForTargetUserId(targetUserId))
                            .filter((castKey, cast) -> Active == cast.status())
                            .groupByKey().aggregate(
                                    () -> new ActiveCast(),
                                    (castKey, cast, aggregate) -> {
                                        log.info("Aggregating {} {}", castKey, cast.toStringFull());
                                        aggregate.activeCast(cast);
                                        return aggregate; },
                                    Materialized.with(new CastKeySerde(), new ActiveCastSerde()));

                    final Properties properties = castSubProperties(CastMicroService.class.getSimpleName(), instanceGroup.instanceId);
                    properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, CastKeySerde.class);
                    properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, CastSerde.class);
                    final KafkaStreams streams = new KafkaStreams(builder.build(), properties);
                    streams.start();

                    activeCasts.toStream().foreach((k, c) ->
                            log.info("targetUserId={} has active cast {}", targetUserId, c.activeCast().toStringFull()));

                    streams.close();

                } else {
                    log.info("Skipping select for targetUserId {}", targetUserId);
                }
            });
        }

        targetedCasts.close();
        casts.close();
        replies.close();
        commands.close();
        active.close();
    }

    private void sendTargetedCast(Cast cast) {
        for (int i = 0; i < cast.targetUserIds().length; i++) {
            final int targetUserId = cast.targetUserIds()[i];
            targetedCasts.send(new ProducerRecord<>(TargetedCasts.name(), targetUserId, cast));
            log.info("Publishing {} key {} {}", TargetedCasts.name(), targetUserId, cast.toStringFull());
        }
    }

    private boolean isCommandForMyOriginators(CommandEvent event) {
        return partitioner.isForInstance(instanceGroup.instanceId, event.key);
    }

    private boolean isAlreadyActive(CastKey key) {
        return cache.containsKey(key) && Active == cache.get(key).status();
    }

    private void replySuccess(int requestId) {
        replies.send(new ProducerRecord<>(Reply.name(), requestId, "Success"));
    }

    private void replyError(int requestId, String message) {
        log.error(message + ". Sending error reply for " + requestId);
        replies.send(new ProducerRecord<>(Reply.name(), requestId, message));
    }

    public void stop() {
        log.info("Shutting down");
        stayAlive.set(false);
    }
}


