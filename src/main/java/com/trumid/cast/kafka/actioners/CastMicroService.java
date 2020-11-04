package com.trumid.cast.kafka.actioners;

import com.trumid.cast.data.Cast;
import com.trumid.cast.data.CastKey;
import com.trumid.cast.kafka.config.CastPartitioner;
import com.trumid.cast.kafka.config.InstanceGroup;
import com.trumid.cast.kafka.events.CommandEvent;
import com.trumid.cast.kafka.events.TargetEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.trumid.cast.client.Client.TARGETED_PARTITIONS;
import static com.trumid.cast.contract.CastStatus.Active;
import static com.trumid.cast.contract.CastStatus.Canceled;
import static com.trumid.cast.kafka.config.Command.Activate;
import static com.trumid.cast.kafka.config.KafkaProperties.*;
import static com.trumid.cast.kafka.config.Topics.*;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;

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
    private final Producer<CastKey, Cast> targetedCasts = new KafkaProducer<>(targetedCastPubProperties());
    private final Consumer<CastKey, Cast> casts;
    private final Consumer<Integer, CommandEvent> commands;
    private final Consumer<Integer, TargetEvent> target;
    private final Producer<Integer, String> replies;

    public CastMicroService() {
        casts = new KafkaConsumer<>(castSubProperties(CastMicroService.class.getSimpleName(), instanceGroup.instanceId));
        commands = new KafkaConsumer<>(commandSubProperties("CommandConsumer-" + instanceGroup.instanceId));
        target = new KafkaConsumer<>(targetSubProperties("TargetConsumer-" + instanceGroup.instanceId));
        replies = new KafkaProducer<>(replyPubProperties());
    }

    public void start() {
        log.info("Consuming casts and commands {}", instanceGroup);

        // I've gone for explicit partition specification, rather than pattern sub - on the basis failover (when added) would be simpler
        casts.assign(asList(new TopicPartition(Casts.name(), instanceGroup.instanceId)));
        commands.subscribe(asList(Commands.name()));
        target.subscribe(asList(Target.name()));

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
                if (! isCommandForMyOriginators(event.key)) {
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

            target.poll(ofMillis(100)).forEach(record -> {
                final Integer requestId = record.key();
                final TargetEvent event = record.value();
                if (!isCommandForMyOriginators(event.key)) {
                    log.info("Skipping {} for {}", event, requestId);
                    return;
                }

                log.info("Processing {} for {}", event, requestId);
                if (! cache.containsKey(event.key)) {
                    replyError(requestId, Target.name() + " received for non-existent cast");
                    return;
                }

                // personally I think we should reject sendCast if the cast is already active - the contract is then that
                // the caller would cancel first and then active with a different list of targets...it's simpler, gets more work
                // out of cancelCast (ie exercises that path, which is a good thing) and avoids the error case of mistakenly
                // activating twice...ie a "copy paste" type error where they meant to activate a new cast but send a cmd for the previous
                final Cast cast = cache.get(event.key);
                if (Active == cast.status()) {
                    replyError(requestId, Activate.name() + " cannot sendCast on already Active cast - cancel first");
                    return;
                }

                cast.targetUserIds(event.targetUserIds);
                sendTargetedCast(cast);
            });
        }

        targetedCasts.close();
        casts.close();
        replies.close();
        commands.close();
        target.close();
    }

    private void sendTargetedCast(Cast cast) {
        for (int i = 0; i < cast.targetUserIds().length; i++) {
            final int targetUserId = cast.targetUserIds()[i];
            final int partition = targetUserId % TARGETED_PARTITIONS;
            targetedCasts.send(new ProducerRecord<>(TargetedCasts.name(), partition, cast.key, cast));
            log.info("Publishing {} key {} partition {}", TargetedCasts.name(), targetUserId, partition, cast.toStringFull());
        }
    }

    private boolean isCommandForMyOriginators(CastKey key) {
        return partitioner.isForInstance(instanceGroup.instanceId, key);
    }

    private boolean isAlreadyActive(CastKey key) {
        return cache.containsKey(key) && Active == cache.get(key).status();
    }

    private void replySuccess(int requestId) {
        log.info("Sending success reply for {}", requestId);
        replies.send(new ProducerRecord<>(Reply.name(), requestId, "Success"));
    }

    private void replyError(int requestId, String message) {
        log.error(message + ". Sending error reply for {}", requestId);
        replies.send(new ProducerRecord<>(Reply.name(), requestId, message));
    }

    public void stop() {
        log.info("Shutting down");
        stayAlive.set(false);
    }
}


