package com.trumid.cast;

import com.trumid.cast.contract.CastService;
import com.trumid.cast.data.Cast;
import com.trumid.cast.data.Fail;
import com.trumid.cast.data.Result;
import com.trumid.cast.data.Success;
import org.junit.Before;
import org.junit.Test;

import static com.trumid.cast.TestUtil.*;
import static com.trumid.cast.contract.CastStatus.*;
import static com.trumid.cast.contract.Side.Buy;
import static com.trumid.cast.contract.Side.Sell;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * I think, for now, tests are the best way to demo the service - plus you get build tests at the service/interface level
 * I've created  a file per method - to make it clearer which use cases are covered
 */
public class CastServiceSendCastTest {

    private CastService service;

    @Before
    public void setUp() {
        service = new StandardCastService();
    }

    @Test
    public void sendCast_validation() {
        Result replacedResult = service.sendCast(generateNewCast(originatorM, bondZ, Sell, targetA).status(Replaced));
        Result canceledResult = service.sendCast(generateNewCast(originatorN, bondX, Buy, targetA).status(Canceled));
        Result nullResult = service.sendCast(null);

        assertThat(replacedResult, instanceOf(Fail.class));
        assertEquals("Cannot send cast with status != Active Cast{key=CastKey{originatorUserId=-1, bondId=101, side=Sell}}", replacedResult.message);
        assertThat(canceledResult, instanceOf(Fail.class));
        assertEquals("Cannot send cast with status != Active Cast{key=CastKey{originatorUserId=-2, bondId=102, side=Buy}}", canceledResult.message);
        assertEquals("Cannot send null cast", nullResult.message);
    }

    @Test
    public void sendCast_validateSuccessfulSend() {
        streamActiveCasts(targetA, service);
        Result result = service.sendCast(generateNewCast(originatorM, bondZ, Sell, targetA));

        assertThat(result, instanceOf(Success.class));
        assertEquals("Success", result.message);
    }

    @Test
    public void sendCast_resultingCastValues() {
        StreamResults results = streamActiveCasts(targetA, service);
        service.sendCast(generateNewCast(originatorM, bondZ, Sell, targetA).price(99).quantity(98));
        sleepTensMs(1);

        Cast result = results.castAt(0);
        assertEquals(originatorM, result.key.originatorUserId);
        assertEquals(bondZ, result.key.bondId);
        assertEquals(Sell, result.key.side);
        assertEquals(99, result.price());
        assertEquals(98, result.quantity());
        assertEquals(Active, result.status());
        assertEquals(1, result.targetUserIds().length);
        assertEquals(targetA, (int)result.targetUserIds()[0]);
    }

    /** we never actually publish a replaced status - it's redundant. a cast is either active or canceled, from the recipient's perspective */
    @Test
    public void sendCast_sendSameKeyTwice() {
        StreamResults results = streamActiveCasts(targetA, service);
        service.sendCast(generateNewCast(originatorM, bondZ, Sell, targetA).price(99).quantity(98));
        sleepTensMs(1);

        Cast initialState = results.castAt(0);
        assertEquals(99, initialState.price());
        assertEquals(98, initialState.quantity());
        assertEquals(Active, initialState.status());

        service.sendCast(generateNewCast(originatorM, bondZ, Sell, targetA).price(97).quantity(96));
        sleepTensMs(1);

        assertEquals(2, results.totalReceived());
        Cast result = results.castAt(1);
        assertEquals(97, result.price());
        assertEquals(96, result.quantity());
        assertEquals(Active, result.status());
    }

    @Test
    public void sendCast_multipleOriginatorsToSingleTarget() {
        StreamResults resultsA = streamActiveCasts(targetA, service);
        StreamResults resultsB = streamActiveCasts(targetB, service);
        service.sendCast(generateNewCast(originatorM, bondX, Buy, targetA).price(55).quantity(56));
        service.sendCast(generateNewCast(originatorN, bondX, Buy, targetA).price(57).quantity(58));
        sleepTensMs(1);

        assertEquals(2, resultsA.totalReceived());
        assertEquals(0, resultsB.totalReceived());

        Cast resultA = resultsA.castAt(0);
        assertEquals(originatorM, resultA.key.originatorUserId);
        assertEquals(55, resultA.price());
        assertEquals(56, resultA.quantity());

        Cast resultB = resultsA.castAt(1);
        assertEquals(originatorN, resultB.key.originatorUserId);
        assertEquals(57, resultB.price());
        assertEquals(58, resultB.quantity());
    }

    // see the TODO in StandardCastService
    @Test
    public void sendCast_twoConsumersOnSameTargetOnlyFirstReceives() {
        StreamResults resultsA = streamActiveCasts(targetA, service);
        StreamResults resultsB = streamActiveCasts(targetA, service); // both on targetA
        service.sendCast(generateNewCast(originatorM, bondX, Buy, targetA));
        sleepTensMs(1);

        assertEquals(1, resultsA.totalReceived());
        assertEquals(0, resultsB.totalReceived());
    }

    @Test
    public void sendCast_doubleSidedQuote() {
        StreamResults results = streamActiveCasts(targetA, service);
        service.sendCast(generateNewCast(originatorM, bondX, Buy, targetA).price(22).quantity(23));
        service.sendCast(generateNewCast(originatorN, bondX, Sell, targetA).price(24).quantity(25));
        sleepTensMs(1);

        assertEquals(2, results.totalReceived());

        Cast buySide = results.castAt(0);
        assertEquals(Buy, buySide.key.side);
        assertEquals(22, buySide.price());
        assertEquals(23, buySide.quantity());

        Cast sellSide = results.castAt(1);
        assertEquals(Sell, sellSide.key.side);
        assertEquals(24, sellSide.price());
        assertEquals(25, sellSide.quantity());
    }

    @Test
    public void cast_toStringFull() {
        Cast cast = generateNewCast(originatorM, bondZ, Buy, targetA)
                .price(1)
                .quantity(2)
                .targetUserIds(3, 4, 5);

        assertEquals("Cast{key=CastKey{originatorUserId=-1, bondId=101, side=Buy}, price=1, quantity=2, status=Active, targetUserIds=[3, 4, 5]}", cast.toStringFull());
    }
}
