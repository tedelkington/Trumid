package com.trumid.cast.kafka.serialize;

import com.trumid.cast.data.CastKey;
import com.trumid.cast.kafka.events.TargetEvent;
import org.apache.kafka.common.serialization.Deserializer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.ArrayList;
import java.util.List;

import static com.trumid.cast.contract.Side.fromFix;
import static com.trumid.cast.kafka.serialize.SerdeUtil.INT_SIZE;
import static com.trumid.cast.kafka.serialize.TargetSerde.*;

public final class TargetDeserializer implements Deserializer<TargetEvent> {
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_SIZE]);
    private final List<Integer> targets = new ArrayList<>(10);

    @Override
    public TargetEvent deserialize(String topic, byte[] bytes) {
        buffer.wrap(bytes);
        targets.clear();

        final CastKey castKey = new CastKey(buffer.getInt(ORIGINATOR_POSITION), buffer.getInt(BOND_POSITION), fromFix(buffer.getInt(SIDE_POSITION)));
        final int numberOfTargets = buffer.getInt(NO_TARGETS_POSITION);
        for (int i = 0; i < numberOfTargets; i++) {
            targets.add(buffer.getInt(TARGETS_POSITION + i * INT_SIZE));
        }

        return new TargetEvent(castKey, targets);
    }
}
