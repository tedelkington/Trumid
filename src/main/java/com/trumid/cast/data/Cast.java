package com.trumid.cast.data;

import com.trumid.cast.contract.CastStatus;
import com.trumid.cast.contract.Side;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.trumid.cast.contract.CastStatus.Undefined;
import static com.trumid.cast.util.Util.UNSET_SET_LONG;
import static java.util.Arrays.stream;

/**
 * A simple cast implementation. In the real world this could be pooled or even a flyweight traversing a buffer, in
 * which case we would add an interface
 */
public final class Cast {
    public final CastKey key;

    private final Set<Integer> targetUserIds = new HashSet<>();

    /** Factored up from a double to a long */
    private long price = UNSET_SET_LONG; // I would actually call this "value" not price - we might want to use the code for yields or swaps even (which are technically "rates") et al
    private long quantity = UNSET_SET_LONG; // (I think "size" is a more agnostic name here also, fyi. no biggie)
    private CastStatus status = Undefined;

    public Cast(int originatorUserId, int bondId, Side side) {
        this.key = new CastKey(originatorUserId, bondId, side);
    }

    public Cast price(long price) {
        this.price = price;
        return this;
    }

    public long price() {
        return price;
    }

    public Cast quantity(long quantity) {
        this.quantity = quantity;
        return this;
    }

    public long quantity() {
        return quantity;
    }

    public Cast status(CastStatus status) {
        this.status = status;
        return this;
    }

    public CastStatus status() {
        return status;
    }

    public Cast targetUserIds(int...targetUserIds) {
        this.targetUserIds.clear();
        stream(targetUserIds).forEach(this.targetUserIds::add);
        return this;
    }

    public Cast targetUserIds(List<Integer> targetUserIds) {
        this.targetUserIds.clear();
        targetUserIds.forEach(this.targetUserIds::add);
        return this;
    }

    public boolean isForTargetUserId(int targetUserId) {
        return targetUserIds.contains(targetUserId);
    }

    public Integer[] targetUserIds() { // the spec has it as an array...
        return targetUserIds.toArray(new Integer[targetUserIds.size()]);
    }

    @Override
    public String toString() {
        return "Cast{key=" + key + '}';
    }

    public String toStringFull() {
        return "Cast{" +
                "key=" + key +
                ", price=" + price +
                ", quantity=" + quantity +
                ", status=" + status +
                ", targetUserIds=" + targetUserIds +
                '}';
    }
}
