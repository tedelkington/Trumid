package com.trumid.cast.kafka.events;

import com.trumid.cast.data.CastKey;
import com.trumid.cast.kafka.config.Command;

import java.util.ArrayList;
import java.util.List;

import static com.trumid.cast.kafka.config.Command.Target;

public final class TargetEvent {
    public final CastKey key;
    public final List<Integer> targetUserIds = new ArrayList<>(10);
    public final Command command = Target;

    public TargetEvent(CastKey key, List<Integer> targetUserIds) {
        this.key = key;
        this.targetUserIds.addAll(targetUserIds);
    }

    @Override
    public String toString() {
        return "TargetEvent{" +
                "key=" + key +
                ", targetUserIds=" + targetUserIds +
                ", command=" + command +
                '}';
    }
}
