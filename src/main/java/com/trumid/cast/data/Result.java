package com.trumid.cast.data;

/**
 * I wouldn't necessarily do this in java (we're new'ing a result on every Cast method call, creating GC etc)
 * but the (Scala) spec does suggest the use of Either's so I'm approximating that in Java via this explicit type...
 */
public abstract class Result {
    public final boolean wasSuccessful;
    public final String message;

    public Result(boolean wasSuccessful, String message) {
        this.wasSuccessful = wasSuccessful;
        this.message = message;
    }

    @Override
    public String toString() {
        return "Result{" +
                "wasSuccessful=" + wasSuccessful +
                ", message='" + message + '\'' +
                '}';
    }
}
