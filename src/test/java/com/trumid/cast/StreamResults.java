package com.trumid.cast;

import com.trumid.cast.data.Cast;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A simple test result collector
 */
public final class StreamResults implements Consumer<Cast> {
    private final List<Cast> results = new ArrayList<>();

    @Override
    public void accept(Cast cast) {
        results.add(cast);
    }

    public Cast castAt(int index) { return results.get(index); }

    public int totalReceived() { return results.size(); }
}
