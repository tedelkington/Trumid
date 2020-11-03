package com.trumid.cast;

import com.trumid.cast.contract.CastService;
import com.trumid.cast.data.Cast;
import org.junit.Before;
import org.junit.Test;

import static com.trumid.cast.TestUtil.*;
import static com.trumid.cast.contract.Side.Buy;
import static org.junit.Assert.assertEquals;

/**
 * I think, for now, tests are the best way to demo the service - plus you get build tests at the service/interface level
 * I've created  a file per method - to make it clearer which use cases are covered
 */
public class CastServiceGetActiveCastsTest {

    private CastService service;

    @Before
    public void setUp() {
        service = new StandardCastService();
    }

    @Test
    public void getActiveCasts_initialState() {
        assertEquals(0, service.getActiveCasts(targetA).length);
        assertEquals(0, service.getActiveCasts(targetB).length);
    }

    @Test
    public void getActiveCasts_successfullyRetrieves() {
        Cast cast = generateNewCast(originatorM, bondZ, Buy, targetA);
        service.sendCast(cast);

        assertEquals(1, service.getActiveCasts(targetA).length);
        assertEquals(0, service.getActiveCasts(targetB).length);
    }

    @Test
    public void getActiveCasts_doesNotRetrieveCanceled() {
        Cast cast = generateNewCast(originatorM, bondZ, Buy, targetA);
        service.sendCast(cast);

        assertEquals(1, service.getActiveCasts(targetA).length);

        service.cancelCast(originatorM, bondZ, Buy);

        assertEquals(0, service.getActiveCasts(targetA).length);
    }
}
