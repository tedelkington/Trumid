package com.trumid.cast.client;

import com.trumid.cast.contract.Side;
import com.trumid.cast.data.CastKey;
import com.trumid.cast.kafka.config.Command;
import com.trumid.cast.kafka.events.CommandEvent;
import com.trumid.cast.kafka.events.TargetEvent;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.trumid.cast.kafka.config.Command.Activate;
import static com.trumid.cast.kafka.config.Command.Cancel;
import static com.trumid.cast.kafka.config.KafkaProperties.*;
import static com.trumid.cast.kafka.config.Topics.*;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * Yikes - I realize JMX is ages old (and pretty terrible API) but to keep things moving I've used it as
 * an example client. It would be a REST service (which I've written simple REST stuff before but in the name of
 * expediency taking this route for now (the spec does say it's our choice ; ) ))
 *
 * I looked at the Spring boot kafka req/reply and the majority of documentation stated that even the latest version
 * has some rough edges - and that even that does not handle multiple clients particularly well...yikes indeed!
 *
 * So this is just a simple, singular, stand alone, hand written example
 */
public final class Client implements ClientMBean {
    private static final Logger log = LoggerFactory.getLogger(Client.class);
    private static final AtomicBoolean stayAlive = new AtomicBoolean(true);

    private final Map<Integer, String> cache = new ConcurrentHashMap<>();
    private final Producer<Integer, CommandEvent> commands = new KafkaProducer<>(commandPubProperties());
    private final Producer<Integer, TargetEvent> targets = new KafkaProducer<>(targetPubProperties());
    private final Producer<Integer, Integer> selects = new KafkaProducer<>(activePubProperties());
    private final Consumer<Integer, String> replies = new KafkaConsumer<>(replySubProperties());

    private int messageId = 1;

    public Client() {
        replies.subscribe(asList(Reply.name()));
    }

    public void doWork() {
        replies.poll(ofMillis(100)).forEach(record -> {
            log.info("Received reply {} {}", record.key(), record.value());
            cache.put(record.key(), record.value());
        });
    }

    @Override
    public String sendCast(int originatorUserId, int bondId, int side) {
        final CastKey key = new CastKey(originatorUserId, bondId, Side.fromFix(side));
        return sendCommand(key, Activate);
    }

    @Override
    public String sendCast(int originatorUserId, int bondId, int side, String targetedUserIdsString) {
        // personally i would have this reject if the cast is already active - the contract is then that
        // the caller would cancel first and then active with a different list of targets...it's simpler, gets more work
        // out of cancelCast (ie exercises that path, which is a good thing) and avoids the error case of mistakenly
        // activating twice...ie a "copy paste" type error where they meant to activate a new cast but send a cmd for the previous
        final CastKey key = new CastKey(originatorUserId, bondId, Side.fromFix(side));
        final List<Integer> targetUserIds = stream(targetedUserIdsString.split(","))
                .map(targetUserId -> Integer.valueOf(targetUserId.trim()))
                .collect(Collectors.toList());

        final int requestId = messageId++;
        log.info("Sending sendCast for {} requestId {} requestId {}", key, targetUserIds, requestId);
        targets.send(new ProducerRecord<>(Target.name(), requestId, new TargetEvent(key, targetUserIds)));

        return waitForReply(requestId, 3);
    }

    @Override
    public String cancelCast(int originatorUserId, int bondId, int side) {
        final CastKey key = new CastKey(originatorUserId, bondId, Side.fromFix(side));
        return sendCommand(key, Cancel);
    }

    @Override
    public String getActiveCasts(int targetUserId) {
        final int requestId = messageId++;
        log.info("Sending getActiveCasts for {} requestId {}", targetUserId, requestId);
        selects.send(new ProducerRecord<>(ActiveCasts.name(), targetUserId, requestId));

        return waitForReply(requestId, 3);
    }

    private String sendCommand(CastKey castKey, Command command) {
        final int requestId = messageId++;
        final CommandEvent event = new CommandEvent(castKey, command);
        log.info("Sending {} is {}", event, requestId);
        commands.send(new ProducerRecord<>(Commands.name(), requestId, event));

        return waitForReply(requestId, 1);
    }

    // TODO this is cr@p I realize
    private String waitForReply(int requestId, int timeoutSeconds) {
        try {
            Thread.sleep(timeoutSeconds * 1000);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }

        final AtomicReference<String> reply = new AtomicReference<>("Timed out");
        cache.computeIfPresent(requestId, (k, v) -> {
            reply.set(cache.remove(k));
            return null;
        });

        return reply.get();
    }

    public static final int TARGETED_PARTITIONS = 10;

    public static void main(String[] args) {
        try {
            final Properties properties = new Properties();
            properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

            final AdminClient admin = AdminClient.create(properties);
            final short noReplication = 1;
            final List<NewTopic> topics = asList(
                    new NewTopic(ActiveCasts.name(), 1, noReplication),
                    new NewTopic(Casts.name(), 3, noReplication),
                    new NewTopic(Commands.name(), 1, noReplication),
                    new NewTopic(Reply.name(), 1, noReplication),
                    new NewTopic(Target.name(), 1, noReplication),
                    new NewTopic(TargetedCasts.name(), TARGETED_PARTITIONS, noReplication));

            admin.createTopics(topics);

            final ObjectName objectName = new ObjectName("com.trumid.cast:name=castService");
            final Client client = new Client();
            getPlatformMBeanServer().registerMBean(client, objectName);

            while (stayAlive.get()) {
                client.doWork();
            }

        } catch (Exception e) {
            log.error("Exiting", e);
        }
    }
}
