package com.trumid.cast.util;

public final class Util {
    // TODO unset values can be tricky in Java for ints and longs (double has NaN at least)
    //  anyway, the purpose here is to have a common, accepted value for unset. On a separate note, the convention of
    //  capitalizing static variables is faulty imo. The client code doesn't care about an entity's "static-ness"...
    //  It's just an entity. But I follow the convention here...
    public static final long UNSET_SET_LONG = Long.MIN_VALUE;

    private Util() {}
}
