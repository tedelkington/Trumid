package com.trumid.cast.kafka.serialize;

import com.trumid.cast.kafka.events.CommandEvent;
import org.apache.kafka.common.serialization.Serializer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static com.trumid.cast.kafka.serialize.CommandSerde.*;

public final class CommandSerializer implements Serializer<CommandEvent> {
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_SIZE]);

    @Override
    public byte[] serialize(String topic, CommandEvent event) {
        buffer.putInt(ORIGINATOR_POSITION, event.key.originatorUserId);
        buffer.putInt(BOND_POSITION, event.key.bondId);
        buffer.putInt(SIDE_POSITION, event.key.side.fixValue());
        buffer.putInt(COMMAND_POSITION, event.command.ordinal());
        return buffer.byteArray();
    }
}
