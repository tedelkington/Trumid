package com.trumid.cast.kafka.actioners;

import com.trumid.cast.data.Cast;

/** Used for now as the aggregation intermediary */
public final class ActiveCast {
    private Cast activeCast;

    public ActiveCast() {
    }

    public ActiveCast(Cast activeCast) {
        this.activeCast = activeCast;
    }

    public Cast activeCast() {
        return activeCast;
    }

    public void activeCast(Cast activeCast) {
        this.activeCast = activeCast;
    }
}
