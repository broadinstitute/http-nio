package org.broadinstitute.http.nio;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.net.ssl.SSLException;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class RetryHandlerUnitTest {
    private static final RetryHandler NO_RETRIES_HANDLER = new RetryHandler(0, Collections.emptySet(),
            Set.of(IOException.class), Collections.emptySet(), e -> false, URI.create("http://example.net"));
    private static final RetryHandler DEFAULT_RETRY_HANDLER = new RetryHandler(HttpFileSystemProviderSettings.DEFAULT_RETRY_SETTINGS, URI.create("http://example.com"));
    @DataProvider
    public static Object[][] getExceptionalConditions() {
        final UnexpectedHttpResponseException retriableUnexpectedResponse = new UnexpectedHttpResponseException(500, "retry");
        final UnexpectedHttpResponseException fatalUnexpectedResponse = new UnexpectedHttpResponseException(100, "fatal");

        return new Object[][]{
                {fatalUnexpectedResponse, false},
                {retriableUnexpectedResponse, true},
                {new IOException(), false},
                {new IOException(retriableUnexpectedResponse), true},
                {new IOException(fatalUnexpectedResponse), false},
                {new SSLException("boop"), true},
                {new EOFException(), true},
                {new SocketException(), true},
                {new SocketTimeoutException(), true},
                {new IOException(new SocketException()), true},
                {new IOException(new ClosedChannelException()), false},
                {new IOException(new IOException()), false},
        };
    }

    @Test(dataProvider = "getExceptionalConditions")
    public void testDefaultIsRetryable(IOException exception, boolean retryable) throws URISyntaxException {
        Assert.assertEquals(DEFAULT_RETRY_HANDLER.isRetryable(exception), retryable);
    }


    @DataProvider
    public Object[][] getForCustomPredicate(){
        return new Object[][] {
                {new UnexpectedHttpResponseException(666,"the beast"), true},
                {new UnexpectedHttpResponseException(666,"party on"), true},
                {new UnexpectedHttpResponseException(668,"the neighbor of the beast"), false},
                {new UnexpectedHttpResponseException(668,"party on"), true},
                {new IOException(), false},
                {new IOException(new IOException("party on")), true},
                {new SocketException(), true},
                {new SocketException("party on"), true},
                {new IOException(new IOException("hello there")), true},
        };
    }
    @Test(dataProvider = "getForCustomPredicate")
    public void testCustomHandler(IOException ex, boolean retryable) throws URISyntaxException {
        final Predicate<Throwable> customPredicate = e -> e.getMessage() != null && e.getMessage().contains("party on");
        final HttpFileSystemProviderSettings.RetrySettings settings = new HttpFileSystemProviderSettings.RetrySettings(1, Set.of(666), Set.of(SocketException.class), Set.of("hello"), customPredicate);
        final RetryHandler retryHandler = new RetryHandler( settings, new URI("http://example.com"));
        Assert.assertEquals(retryHandler.isRetryable(ex), retryable);
    }

    @Test
    public void testSucessful() throws IOException {
        Assert.assertEquals(DEFAULT_RETRY_HANDLER.runWithRetries(() -> 3), 3);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testUnretryableRuntimeException() throws IOException {
      DEFAULT_RETRY_HANDLER.runWithRetries(() -> {
            throw new RuntimeException("can't retry this");
      });
    }

    @Test(expectedExceptions = OutOfRetriesException.class)
    public void testRetryableButAlwaysFails() throws IOException {
        try{
            DEFAULT_RETRY_HANDLER.runWithRetries(() -> {
                throw new SocketException();
            });
        } catch (OutOfRetriesException e){
            Assert.assertEquals(e.getRetries(), DEFAULT_RETRY_HANDLER.getMaxRetries());
            Assert.assertTrue(e.getTotalWaitTime().toMillis() > 0);
            Assert.assertEquals(e.getCause().getClass(), SocketException.class);
            throw e;
        }
    }

    @Test
    public void testTryOnceAndThenInitialSuccess() throws IOException {
        AtomicInteger a = new AtomicInteger(0);
        AtomicInteger b = new AtomicInteger(10);
        Assert.assertEquals(DEFAULT_RETRY_HANDLER.tryOnceThenWithRetries(
                a::incrementAndGet,
                b::incrementAndGet
        ), 1);
    }

    @Test
    public void testTryOnceAndThenInitialRetryableFailure() throws IOException {
        AtomicInteger a = new AtomicInteger(0);
        AtomicInteger b = new AtomicInteger(10);
        Assert.assertEquals(DEFAULT_RETRY_HANDLER.tryOnceThenWithRetries(
                () -> { a.incrementAndGet(); throw new SocketException("I am retryable");},
                b::incrementAndGet
        ), 11);
        Assert.assertEquals(a.get(), 1);
    }

    @Test
    public void testTryOnceAndThenInitialNotRetryable() throws IOException {
        AtomicInteger a = new AtomicInteger(0);
        AtomicInteger b = new AtomicInteger(10);
        Assert.assertThrows(IOException.class, () -> DEFAULT_RETRY_HANDLER.tryOnceThenWithRetries(
                () -> { a.incrementAndGet(); throw new IOException("boom");},
                b::incrementAndGet
        ));
        Assert.assertEquals(a.get(), 1);
    }

    @Test
    public void testTryOnceAndThenNoRetrysAvailable() throws IOException {
        AtomicInteger a = new AtomicInteger(0);
        AtomicInteger b = new AtomicInteger(10);
        Assert.assertThrows(OutOfRetriesException.class, () -> NO_RETRIES_HANDLER.tryOnceThenWithRetries(
                () -> { a.incrementAndGet(); throw new IOException("I'm retryable by this handler");},
                b::incrementAndGet
        ));
        Assert.assertEquals(a.get(), 1);
        Assert.assertEquals(b.get(), 10);
    }

    @Test
    public void testTryOnceAndThenFailFirstRetry() throws IOException {
        AtomicInteger a = new AtomicInteger(0);
        AtomicInteger b = new AtomicInteger(10);
        Assert.assertEquals(DEFAULT_RETRY_HANDLER.tryOnceThenWithRetries(
                () -> { a.incrementAndGet(); throw new SocketException("I'm retryable");},
                () -> {
                    if(b.incrementAndGet() <= 11){
                        throw new SocketException("I'm retryable");
                    }
                    return b.get();
                }
        ), 12);
        Assert.assertEquals(a.get(), 1);
        Assert.assertEquals(b.get(), 12);
    }

    @Test
    public void testTryOnceAndThenFailEveryRetry() throws IOException {
        AtomicInteger a = new AtomicInteger(0);
        AtomicInteger b = new AtomicInteger(10);
        Assert.assertThrows(OutOfRetriesException.class,
                () -> DEFAULT_RETRY_HANDLER.tryOnceThenWithRetries(
                    () -> { a.incrementAndGet(); throw new SocketException("I'm retryable");},
                    () -> { b.incrementAndGet(); throw new SocketException("I'm retryable");}
                ));
        Assert.assertEquals(a.get(), 1);
        Assert.assertEquals(b.get(), 10+DEFAULT_RETRY_HANDLER.getMaxRetries());
    }


    @Test
    public void test0MaxRetriesRunsOnce() throws IOException {
        Assert.assertEquals(NO_RETRIES_HANDLER.runWithRetries(() -> 3), 3);
    }

    @Test
    public void test0MaxRetriesDoesntRetry() throws IOException {
        AtomicInteger count = new AtomicInteger(0);

        try {
            NO_RETRIES_HANDLER.runWithRetries(() -> {
                count.incrementAndGet();
                throw new IOException();
            });
        } catch (OutOfRetriesException e){
            Assert.assertEquals(e.getRetries(), 0);
            Assert.assertEquals(count.get(), 1);
        }
    }


}