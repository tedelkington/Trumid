package com.trumid.cast.kafka.serialize;

import com.trumid.cast.data.Cast;
import org.apache.kafka.common.serialization.Deserializer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static com.trumid.cast.contract.CastStatus.from;
import static com.trumid.cast.contract.Side.fromFix;
import static com.trumid.cast.kafka.serialize.CastSerde.*;
import static com.trumid.cast.kafka.serialize.SerdeUtil.INT_SIZE;

public final class CastDeserializer implements Deserializer<Cast> {
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_SIZE]);

    @Override
    public Cast deserialize(String topic, byte[] data) {
        buffer.wrap(data);

        final Cast cast = new Cast(buffer.getInt(ORIGINATOR_POSITION), buffer.getInt(BOND_POSITION), fromFix(buffer.getInt(SIDE_POSITION)));
        cast.price(buffer.getLong(PRICE_POSITION))
                .quantity(buffer.getLong(QUANTITY_POSITION))
                .status(from(buffer.getInt(STATUS_POSITION)));

        final int noOfTargets = buffer.getInt(NO_TARGETS_POSITION);
        final int[] targetUserIds = new int[noOfTargets];
        for (int i = 0; i < noOfTargets; i++) {
            targetUserIds[i] = buffer.getInt(TARGETS_POSITION + i * INT_SIZE);
        }

        return cast.targetUserIds(targetUserIds);
    }
}
