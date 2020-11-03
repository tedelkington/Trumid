package com.trumid.cast.kafka.serialize;

import com.trumid.cast.kafka.events.CommandEvent;
import com.trumid.cast.kafka.events.TargetEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import static com.trumid.cast.kafka.serialize.SerdeUtil.INT_SIZE;

public final class TargetSerde implements Serde<TargetEvent> {
    public static final int ORIGINATOR_POSITION = 0;
    public static final int BOND_POSITION = ORIGINATOR_POSITION + INT_SIZE;
    public static final int SIDE_POSITION = BOND_POSITION + INT_SIZE;
    public static final int NO_TARGETS_POSITION = SIDE_POSITION + INT_SIZE;
    public static final int TARGETS_POSITION = NO_TARGETS_POSITION + 10 * INT_SIZE; // TODO for now, i'm arbitrarily making 10 the max number of targets
    public static final int MAX_SIZE = TARGETS_POSITION + INT_SIZE;

    private final TargetSerializer serializer = new TargetSerializer();
    private final TargetDeserializer deserializer = new TargetDeserializer();

    @Override
    public Serializer<TargetEvent> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<TargetEvent> deserializer() {
        return deserializer;
    }
}
