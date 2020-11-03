package com.trumid.cast.kafka.serialize;

import com.trumid.cast.data.Cast;
import org.apache.kafka.common.serialization.Serializer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static com.trumid.cast.kafka.serialize.CastSerde.*;
import static com.trumid.cast.kafka.serialize.SerdeUtil.INT_SIZE;

public final class CastSerializer implements Serializer<Cast> {
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_SIZE]);

    @Override
    public byte[] serialize(String topic, Cast cast) {
        final int noOfTargets = cast.targetUserIds().length;
        buffer.putInt(ORIGINATOR_POSITION, cast.key.originatorUserId);
        buffer.putInt(BOND_POSITION, cast.key.bondId);
        buffer.putInt(SIDE_POSITION, cast.key.side.fixValue());
        buffer.putLong(PRICE_POSITION, cast.price());
        buffer.putLong(QUANTITY_POSITION, cast.quantity());
        buffer.putInt(STATUS_POSITION, cast.status().ordinal());
        buffer.putInt(NO_TARGETS_POSITION, noOfTargets);
        for (int i = 0; i < noOfTargets; i++) {
            buffer.putInt(TARGETS_POSITION + i * INT_SIZE, cast.targetUserIds()[i]);
        }
        return buffer.byteArray();
    }
}
