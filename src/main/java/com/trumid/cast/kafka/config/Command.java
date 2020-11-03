package com.trumid.cast.kafka.config;

import java.util.Map;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toMap;

public enum Command {
    Activate,
    Cancel;

    private final static Map<Integer, Command> values = stream(values()).collect(toMap(Enum::ordinal, value -> value));

    public static Command from(int ordinal) {
        return values.get(ordinal);
    }
}
