package com.trumid.cast;

import com.trumid.cast.contract.CastService;
import com.trumid.cast.contract.Side;
import com.trumid.cast.data.Cast;

import java.util.function.Consumer;

import static com.trumid.cast.contract.CastStatus.Active;
import static java.lang.Thread.sleep;

/**
 * Some test utilities
 */
public final class TestUtil {

    public static final int targetA = 1;
    public static final int targetB = 2;
    public static final int originatorM = -1;
    public static final int originatorN = -2;
    public static final int bondZ = 101;
    public static final int bondX = 102;

    /** defaults the price and quantity and sets status to active */
    public static Cast generateNewCast(int originatorUserId, int bondId, Side side, int...targetUserIds) {
        return new Cast(originatorUserId, bondId, side)
                .price(100)
                .quantity(1_000_000)
                .status(Active)
                .targetUserIds(targetUserIds);
    }

    /** calls {@link CastService#streamActiveCasts(int, Consumer)} for this targetUserId on new Thread */
    public static StreamResults streamActiveCasts(int targetUserId, CastService service) {
        StreamResults results = new StreamResults();
        new Thread(() -> service.streamActiveCasts(targetUserId, results)).start();
        return results;
    }

    /** sleeps current thread for specified tens of milliseconds */
    public static boolean sleepTensMs(int tensOfMillis) {
        try {
            sleep(100 * tensOfMillis);
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    private TestUtil() {}
}
