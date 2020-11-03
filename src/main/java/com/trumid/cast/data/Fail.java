package com.trumid.cast.data;

/**
 * {@see Result}
 */
public final class Fail extends Result {
    public Fail(String message) {
        super(false, message);
    }
}
