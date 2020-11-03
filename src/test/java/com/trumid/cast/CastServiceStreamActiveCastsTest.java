package com.trumid.cast;

import com.trumid.cast.contract.CastService;
import com.trumid.cast.data.Cast;
import org.junit.Before;
import org.junit.Test;

import static com.trumid.cast.TestUtil.*;
import static com.trumid.cast.contract.CastStatus.Active;
import static com.trumid.cast.contract.Side.Buy;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * I think, for now, tests are the best way to demo the service - plus you get build tests at the service/interface level
 * I've created  a file per method - to make it clearer which use cases are covered
 */
public class CastServiceStreamActiveCastsTest {

    private CastService service;

    @Before
    public void setUp() {
        service = new StandardCastService();
    }

    @Test
    public void streamActiveCasts_validation() {
        Exception exception = assertThrows(NullPointerException.class, () -> service.streamActiveCasts(targetA, null));
        assertEquals("Consumer is null for targetUserId=1", exception.getMessage());
    }

    @Test
    public void streamActiveCasts_twoCastsReceivedWhenBothTargetUserIdsSpecified() {
        StreamResults resultsA = streamActiveCasts(targetA, service);
        StreamResults resultsB = streamActiveCasts(targetB, service);
        service.sendCast(generateNewCast(originatorM, bondZ, Buy, targetA, targetB));
        sleepTensMs(1);

        assertEquals(1, resultsA.totalReceived());
        assertEquals(1, resultsB.totalReceived());
        assertEquals(targetA, (int)resultsA.castAt(0).targetUserIds()[0]);
        assertEquals(targetB, (int)resultsA.castAt(0).targetUserIds()[1]);
    }

    @Test
    public void streamActiveCasts_oneCastReceivedWhenOnlyOneTargetUserIdSpecified() {
        StreamResults resultsA = streamActiveCasts(targetA, service);
        StreamResults resultsB = streamActiveCasts(targetB, service);
        service.sendCast(generateNewCast(originatorM, bondZ, Buy, targetA));
        sleepTensMs(1);

        assertEquals(1, resultsA.totalReceived());
        assertEquals(0, resultsB.totalReceived());
        assertEquals(targetA, (int)resultsA.castAt(0).targetUserIds()[0]);
    }

    @Test
    public void streamActiveCasts_emptyTargetIdNoOneReceives() {
        StreamResults resultsA = streamActiveCasts(targetA, service);
        StreamResults resultsB = streamActiveCasts(targetB, service);
        service.sendCast(new Cast(originatorM, bondZ, Buy).price(100).quantity(1_000_000).status(Active));
        sleepTensMs(1);

        assertEquals(0, resultsA.totalReceived());
        assertEquals(0, resultsB.totalReceived());
    }

    @Test
    public void streamActiveCasts_initiallyEmptyTargetIdReceivedOncePopulated() {
        StreamResults resultsA = streamActiveCasts(targetA, service);
        StreamResults resultsB = streamActiveCasts(targetB, service);
        service.sendCast(new Cast(originatorM, bondZ, Buy).price(100).quantity(1_000_000).status(Active));
        sleepTensMs(1);

        assertEquals(0, resultsA.totalReceived());
        assertEquals(0, resultsB.totalReceived());

        service.sendCast(new Cast(originatorM, bondZ, Buy).price(100).quantity(1_000_000).status(Active).targetUserIds(targetA, targetB));
        sleepTensMs(1);

        assertEquals(1, resultsA.totalReceived());
        assertEquals(1, resultsB.totalReceived());
        assertEquals(targetA, (int)resultsA.castAt(0).targetUserIds()[0]);
        assertEquals(targetB, (int)resultsA.castAt(0).targetUserIds()[1]);
    }
}
