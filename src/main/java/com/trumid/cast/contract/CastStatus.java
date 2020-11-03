package com.trumid.cast.contract;

import java.util.Map;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toMap;

public enum CastStatus {
    Active,
    Canceled,
    Replaced,
    Undefined;

    private final static Map<Integer, CastStatus> values = stream(values()).collect(toMap(Enum::ordinal, value -> value));

    public static CastStatus from(int ordinal) {
        return values.get(ordinal);
    }
}
