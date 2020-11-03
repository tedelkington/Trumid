package com.trumid.cast.contract;

import com.trumid.cast.data.Cast;
import com.trumid.cast.data.Result;

import java.util.function.Consumer;

/**
 * A Cast is a mechanism for broker dealers (originators) to indicate quantities of bonds they'd like to Buy/Sell at
 * a given price. The Targets of Casts are client traders
 */
// TODO - I would prob simplify the names slightly ie "originatorId", "targetId" and bondId -> "instrumentId" but sticking with the spec
public interface CastService {

    Result sendCast(Cast cast);

    Result cancelCast(int originatorUserId, int bondId, Side side);

    Cast[] getActiveCasts(int targetUserId); // I would return a List rather than an array

    void streamActiveCasts(int targetUserId, Consumer<Cast> consumer);
}