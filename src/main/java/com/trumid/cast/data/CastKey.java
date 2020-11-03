package com.trumid.cast.data;

import com.trumid.cast.contract.Side;

import java.util.Objects;

public final class CastKey {
    public final int originatorUserId;
    public final int bondId; // could be called "instrumentId" - we might expand beyond bonds...
    public final Side side;

    public CastKey(int originatorUserId, int bondId, Side side) {
        this.originatorUserId = originatorUserId;
        this.bondId = bondId;
        this.side = side;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CastKey castKey = (CastKey) o;
        return originatorUserId == castKey.originatorUserId &&
                bondId == castKey.bondId &&
                side == castKey.side;
    }

    @Override
    public int hashCode() {
        return Objects.hash(originatorUserId, bondId, side);
    }

    @Override
    public String toString() {
        return "CastKey{" +
                "originatorUserId=" + originatorUserId +
                ", bondId=" + bondId +
                ", side=" + side +
                '}';
    }
}
