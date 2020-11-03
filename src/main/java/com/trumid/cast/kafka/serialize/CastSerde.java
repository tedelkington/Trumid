package com.trumid.cast.kafka.serialize;

import com.trumid.cast.data.Cast;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import static com.trumid.cast.kafka.serialize.SerdeUtil.INT_SIZE;
import static com.trumid.cast.kafka.serialize.SerdeUtil.LONG_SIZE;

public final class CastSerde implements Serde<Cast> {
    public static final int ORIGINATOR_POSITION = 0;
    public static final int BOND_POSITION = ORIGINATOR_POSITION + INT_SIZE;
    public static final int SIDE_POSITION = BOND_POSITION + INT_SIZE;
    public static final int PRICE_POSITION = SIDE_POSITION + INT_SIZE;
    public static final int QUANTITY_POSITION = PRICE_POSITION + LONG_SIZE;
    public static final int STATUS_POSITION = QUANTITY_POSITION + LONG_SIZE;
    public static final int NO_TARGETS_POSITION = STATUS_POSITION + INT_SIZE;
    public static final int TARGETS_POSITION = NO_TARGETS_POSITION + INT_SIZE;
    public static final int MAX_SIZE = TARGETS_POSITION + 10 * INT_SIZE; // TODO for now, i'm arbitrarily making 10 the max number of targets

    private final CastSerializer serializer = new CastSerializer();
    private final CastDeserializer deserializer = new CastDeserializer();

    @Override
    public Serializer<Cast> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Cast> deserializer() {
        return deserializer;
    }
}
