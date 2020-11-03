package com.trumid.cast.kafka.serialize;

import com.trumid.cast.kafka.events.CommandEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import static com.trumid.cast.kafka.serialize.SerdeUtil.INT_SIZE;

public final class CommandSerde implements Serde<CommandEvent> {
    public static final int ORIGINATOR_POSITION = 0;
    public static final int BOND_POSITION = ORIGINATOR_POSITION + INT_SIZE;
    public static final int SIDE_POSITION = BOND_POSITION + INT_SIZE;
    public static final int COMMAND_POSITION = SIDE_POSITION + INT_SIZE;
    public static final int MAX_SIZE = COMMAND_POSITION + INT_SIZE;

    private final CommandSerializer serializer = new CommandSerializer();
    private final CommandDeserializer deserializer = new CommandDeserializer();

    @Override
    public Serializer<CommandEvent> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<CommandEvent> deserializer() {
        return deserializer;
    }
}
