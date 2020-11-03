package com.trumid.cast;

import com.trumid.cast.contract.CastService;
import com.trumid.cast.data.Fail;
import com.trumid.cast.data.Result;
import com.trumid.cast.data.Success;
import org.junit.Before;
import org.junit.Test;

import static com.trumid.cast.TestUtil.*;
import static com.trumid.cast.contract.CastStatus.Active;
import static com.trumid.cast.contract.CastStatus.Canceled;
import static com.trumid.cast.contract.Side.Buy;
import static com.trumid.cast.contract.Side.Sell;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * I think, for now, tests are the best way to demo the service - plus you get build tests at the service/interface level
 * I've created  a file per method - to make it clearer which use cases are covered
 */
public class CastServiceCancelCastTest {

    private CastService service;

    @Before
    public void setUp() {
        service = new StandardCastService();
    }

    @Test
    public void cancelCast_nullSideFails() {
        Result result = service.cancelCast(originatorM, bondX, null);

        assertThat(result, instanceOf(Fail.class));
        assertEquals("Side null for originatorUserId=-1, bondId=102", result.message);
    }

    @Test
    public void cancelCast_failsForNonExistingCast() {
        Result result = service.cancelCast(originatorM, bondX, Buy);

        assertThat(result, instanceOf(Fail.class));
        assertEquals("No cast to cancel for CastKey{originatorUserId=-1, bondId=102, side=Buy}", result.message);
    }

    @Test
    public void cancelCast_successfullyCancels() {
        StreamResults results = streamActiveCasts(targetA, service);
        service.sendCast(generateNewCast(originatorM, bondZ, Sell, targetA));
        sleepTensMs(1);

        assertEquals(Active, results.castAt(0).status());

        Result cancelResult = service.cancelCast(originatorM, bondZ, Sell);
        sleepTensMs(1);

        assertEquals(Canceled, results.castAt(0).status());
        assertThat(cancelResult, instanceOf(Success.class));
        assertEquals("Success", cancelResult.message);
    }
}
