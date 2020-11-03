package com.trumid.cast.kafka.serialize;

import com.trumid.cast.data.CastKey;
import org.apache.kafka.common.serialization.Serializer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static com.trumid.cast.kafka.serialize.CastKeySerde.*;

public final class CastKeySerializer implements Serializer<CastKey> {
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_SIZE]);

    @Override
    public byte[] serialize(String topic, CastKey castKey) {
        buffer.putInt(ORIGINATOR_POSITION, castKey.originatorUserId);
        buffer.putInt(BOND_POSITION, castKey.bondId);
        buffer.putInt(SIDE_POSITION, castKey.side.fixValue());
        return buffer.byteArray();
    }
}
