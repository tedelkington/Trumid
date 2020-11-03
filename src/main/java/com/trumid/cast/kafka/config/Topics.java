package com.trumid.cast.kafka.config;

public enum Topics {
    /** getActiveCasts() commands */
    ActiveCasts,
    /** the initial Cast stream that we subscribe to */
    Casts,
    /** sendCast() and cancelCast() commands */
    Commands,
    /** all command replies */
    Reply,
    /** new sendCast() implementation that specifies a new targetUserIds list */
    Target,
    /** the targeted casts, partitions by the modulo of each of their targets */
    TargetedCasts
}
