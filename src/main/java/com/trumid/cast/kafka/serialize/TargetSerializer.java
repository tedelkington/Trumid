package com.trumid.cast.kafka.serialize;

import com.trumid.cast.kafka.events.TargetEvent;
import org.apache.kafka.common.serialization.Serializer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.List;

import static com.trumid.cast.kafka.serialize.SerdeUtil.INT_SIZE;
import static com.trumid.cast.kafka.serialize.TargetSerde.*;

public final class TargetSerializer implements Serializer<TargetEvent> {
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_SIZE]);

    @Override
    public byte[] serialize(String topic, TargetEvent event) {
        buffer.putInt(ORIGINATOR_POSITION, event.key.originatorUserId);
        buffer.putInt(BOND_POSITION, event.key.bondId);
        buffer.putInt(SIDE_POSITION, event.key.side.fixValue());
        final List<Integer> targetUserIds = event.targetUserIds;
        buffer.putInt(NO_TARGETS_POSITION, targetUserIds.size());
        for (int i = 0; i < targetUserIds.size(); i++) {
            buffer.putInt(TARGETS_POSITION + i * INT_SIZE, targetUserIds.get(i));
        }

        return buffer.byteArray();
    }
}
