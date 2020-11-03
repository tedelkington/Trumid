package com.trumid.cast.kafka.events;

import com.trumid.cast.data.CastKey;
import com.trumid.cast.kafka.config.Command;

public final class CommandEvent {
    public final CastKey key;
    public final Command command;

    public CommandEvent(CastKey key, Command command) {
        this.key = key;
        this.command = command;
    }

    @Override
    public String toString() {
        return "CommandEvent{" +
                "key=" + key +
                ", command=" + command +
                '}';
    }
}
