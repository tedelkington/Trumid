package com.trumid.cast.kafka.serialize;

import com.trumid.cast.data.CastKey;
import com.trumid.cast.kafka.config.Command;
import com.trumid.cast.kafka.events.CommandEvent;
import org.apache.kafka.common.serialization.Deserializer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static com.trumid.cast.contract.Side.fromFix;
import static com.trumid.cast.kafka.serialize.CommandSerde.*;

public final class CommandDeserializer implements Deserializer<CommandEvent> {
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_SIZE]);

    @Override
    public CommandEvent deserialize(String topic, byte[] bytes) {
        buffer.wrap(bytes);
        final CastKey castKey = new CastKey(buffer.getInt(ORIGINATOR_POSITION), buffer.getInt(BOND_POSITION), fromFix(buffer.getInt(SIDE_POSITION)));
        return new CommandEvent(castKey, Command.from(buffer.getInt(COMMAND_POSITION)));
    }
}
