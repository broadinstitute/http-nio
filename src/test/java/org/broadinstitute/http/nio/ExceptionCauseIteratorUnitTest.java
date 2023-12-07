package org.broadinstitute.http.nio;

import org.broadinstitute.http.nio.utils.ExceptionCauseIterator;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.NoSuchElementException;

public class ExceptionCauseIteratorUnitTest {

    @Test(timeOut = 100)
    public void checkRecursiveCauseEnds(){
        final IOException recursive = new IOException();
        final IOException recursive2 = new IOException();
        recursive.initCause(recursive2);
        recursive2.initCause(recursive);

        int count = 0;
        for(Throwable ex :new ExceptionCauseIterator(recursive)){
            count++;
        }
        Assert.assertEquals(count, ExceptionCauseIterator.MAX_DEPTH);
    }

    @Test
    public void testCauseChain(){
        IOException deepest = new IOException("deepest");
        IOException secondDeepest = new IOException("second deepest", deepest);
        Exception mid = new Exception("mid", secondDeepest);
        Exception nearTheTop = new Exception("so close", mid);
        Throwable top = new Throwable("we made it", nearTheTop);
        final ExceptionCauseIterator causes = new ExceptionCauseIterator(top);

        Assert.assertTrue(causes.hasNext());
        Assert.assertEquals(causes.next(), top);

        Assert.assertTrue(causes.hasNext());
        Assert.assertEquals(causes.next(), nearTheTop);

        Assert.assertTrue(causes.hasNext());
        Assert.assertEquals(causes.next(), mid);

        Assert.assertTrue(causes.hasNext());
        Assert.assertEquals(causes.next(), secondDeepest);

        Assert.assertTrue(causes.hasNext());
        Assert.assertEquals(causes.next(), deepest);

        Assert.assertFalse(causes.hasNext());
        Assert.assertThrows(NoSuchElementException.class, causes::next);
    }

    @Test
    public void testFirstHasNoCause() {
        final IOException ex = new IOException();
        final ExceptionCauseIterator iter = new ExceptionCauseIterator(ex);
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(iter.next(), ex);

        Assert.assertFalse(iter.hasNext());
    }
}