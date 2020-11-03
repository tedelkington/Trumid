package com.trumid.cast.kafka.serialize;

import com.trumid.cast.kafka.actioners.ActiveCast;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public final class ActiveCastSerde implements Serde<ActiveCast> {
    private final CastSerde castSerde = new CastSerde();

    @Override
    public Serializer<ActiveCast> serializer() {
        return (topic, activeCast) -> castSerde.serializer().serialize(topic, activeCast.activeCast());
    }

    @Override
    public Deserializer<ActiveCast> deserializer() {
        return (topic, bytes) ->
            new ActiveCast(castSerde.deserializer().deserialize(topic, bytes));
    }
}
