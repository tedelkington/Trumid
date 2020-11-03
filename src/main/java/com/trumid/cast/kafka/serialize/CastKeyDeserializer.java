package com.trumid.cast.kafka.serialize;

import com.trumid.cast.data.CastKey;
import org.apache.kafka.common.serialization.Deserializer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static com.trumid.cast.contract.Side.fromFix;
import static com.trumid.cast.kafka.serialize.CastKeySerde.*;

public final class CastKeyDeserializer implements Deserializer<CastKey> {
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_SIZE]);

    @Override
    public CastKey deserialize(String topic, byte[] data) {
        buffer.wrap(data);
        return new CastKey(buffer.getInt(ORIGINATOR_POSITION), buffer.getInt(BOND_POSITION), fromFix(buffer.getInt(SIDE_POSITION)));
    }
}
