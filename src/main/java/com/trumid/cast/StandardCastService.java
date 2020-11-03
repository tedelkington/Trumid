package com.trumid.cast;

import com.aol.simple.react.async.Queue;
import com.trumid.cast.contract.CastService;
import com.trumid.cast.contract.Side;
import com.trumid.cast.data.*;
import com.trumid.cast.kafka.actioners.CastMicroService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.trumid.cast.contract.CastStatus.Active;
import static com.trumid.cast.contract.CastStatus.Canceled;
import static java.util.Arrays.stream;

/** My original and now legacy implementation - see {@link CastMicroService} */
@Deprecated
public final class StandardCastService implements CastService {
    private final Logger log = LoggerFactory.getLogger(StandardCastService.class);
    // TODO I'm sure there's a more reactive way of doing this, ie having a map Queues
    //  *i'm very keen to learn and study more about reactive programming and Scala in general*
    // TODO also, I think it's legitimate this has the limitation that if there's multiple consumers for the same
    //  CastKey-AND-targetId only one will get called (the first). To me this makes sense - in a protocol where
    //  the sender specifies targetId(s), those ids represent distinct nodes and a single consumer...(the node can multiplex internally if it wants)
    //  ...or perhaps not? perhaps in reactive-world this is accepted and common place? Anyway I'm keen to learn
    private final Map<Integer, Queue<Cast>> targets = new HashMap<>();
    private final Map<CastKey, Cast> casts = new HashMap<>();
    private final Function<Integer, Queue<Cast>> newQueue = targetId -> new Queue<>(); // multiple constructors so cannot use Queue::new

    @Override
    public Result sendCast(Cast cast) {
        if (null == cast) {
            return new Fail("Cannot send null cast");
        }

        if (Active != cast.status()) {
            return new Fail("Cannot send cast with status != Active " + cast);
        }

        casts.put(cast.key, cast);
        return broadcast(cast);
    }

    @Override
    public Result cancelCast(int originatorUserId, int bondId, Side side) {
        if (null == side) {
            return new Fail("Side null for originatorUserId=" + originatorUserId + ", bondId=" + bondId);
        }

        final CastKey key = new CastKey(originatorUserId, bondId, side);
        final Cast cast = casts.get(key);

        if (cast == null) {
            return new Fail("No cast to cancel for " + key);
        }

        cast.status(Canceled);
        return broadcast(cast);
    }

    @Override
    public Cast[] getActiveCasts(int targetUserId) {
        return casts.values().stream()
                .filter(cast -> cast.isForTargetUserId(targetUserId)) // it's more likely that's it's not for me than it being in canceled state
                .filter(cast -> Canceled != cast.status())
                .toArray(size -> new Cast[size]);
    }

    @Override
    public void streamActiveCasts(int targetUserId, Consumer<Cast> consumer) {
        if (consumer == null) {
            throw new NullPointerException("Consumer is null for targetUserId=" + targetUserId);
        }

        targets.computeIfAbsent(targetUserId, newQueue).stream()
                .forEach(cast -> consumer.accept(cast));
    }

    // TODO with a more fully/truly reactive set up we'd handle exceptions via channels. I'm just mimic'ing
    //  scala's Either via simple Java return values, since the spec suggests Either's (notwithstanding the GC created)
    private Result broadcast(Cast cast) {
        if (log.isTraceEnabled()) {
            log.trace("Broadcasting {}", cast.toStringFull());
        }

        try {
            stream(cast.targetUserIds())
                    .map(targetId -> targets.computeIfAbsent(targetId, newQueue))
                    .forEach(queue -> queue.add(cast));

            return new Success();

        } catch (Exception e) {
            return new Fail("Failed to send cast " + cast + " " + e.getMessage());
        }
    }
}
