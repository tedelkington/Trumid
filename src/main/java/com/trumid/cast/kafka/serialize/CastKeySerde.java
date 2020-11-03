package com.trumid.cast.kafka.serialize;

import com.trumid.cast.data.CastKey;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import static com.trumid.cast.kafka.serialize.SerdeUtil.INT_SIZE;

public final class CastKeySerde implements Serde<CastKey> {
    public static final int ORIGINATOR_POSITION = 0;
    public static final int BOND_POSITION = ORIGINATOR_POSITION + INT_SIZE;
    public static final int SIDE_POSITION = BOND_POSITION + INT_SIZE;
    public static final int MAX_SIZE = SIDE_POSITION + INT_SIZE;

    private final CastKeySerializer serializer = new CastKeySerializer();
    private final CastKeyDeserializer deserializer = new CastKeyDeserializer();

    @Override
    public Serializer<CastKey> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<CastKey> deserializer() {
        return deserializer;
    }
}
