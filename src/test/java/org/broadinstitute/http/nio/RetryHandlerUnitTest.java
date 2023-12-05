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
    final RetryHandler defaultRetryHandler = new RetryHandler(HttpFileSystemProviderSettings.DEFAULT_RETRY_SETTINGS, URI.create("http://example.com"));
    @DataProvider
    public static Object[][] getExceptionalConditions() {
        final HttpSeekableByteChannel.UnexpectedHttpResponseException retriableUnexpectedResponse = new HttpSeekableByteChannel.UnexpectedHttpResponseException(500, "retry");
        final HttpSeekableByteChannel.UnexpectedHttpResponseException fatalUnexpectedResponse = new HttpSeekableByteChannel.UnexpectedHttpResponseException(100, "fatal");

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
        Assert.assertEquals(defaultRetryHandler.isRetryable(exception), retryable);
    }


    @DataProvider
    public Object[][] getForCustomPredicate(){
        return new Object[][] {
                {new HttpSeekableByteChannel.UnexpectedHttpResponseException(666,"the beast"), true},
                {new HttpSeekableByteChannel.UnexpectedHttpResponseException(666,"party on"), true},
                {new HttpSeekableByteChannel.UnexpectedHttpResponseException(668,"the neighbor of the beast"), false},
                {new HttpSeekableByteChannel.UnexpectedHttpResponseException(668,"party on"), true},
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
        Assert.assertEquals(defaultRetryHandler.runWithRetries(() -> 3), 3);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testUnretryableRuntimeException() throws IOException {
      defaultRetryHandler.runWithRetries(() -> {
            throw new RuntimeException("can't retry this");
      });
    }

    @Test(expectedExceptions = OutOfRetriesException.class)
    public void testRetryableButAlwaysFails() throws IOException {
        try{
            defaultRetryHandler.runWithRetries(() -> {
                throw new SocketException();
            });
        } catch (OutOfRetriesException e){
            Assert.assertEquals(e.getRetries(), defaultRetryHandler.getMaxRetries());
            Assert.assertTrue(e.getTotalWaitTime().toMillis() > 0);
            Assert.assertEquals(e.getCause().getClass(), SocketException.class);
            throw e;
        }
    }

    @Test
    public void test0MaxRetriesRunsOnce() throws IOException {
        final RetryHandler retryHandler = new RetryHandler(0, Collections.emptySet(),
                Set.of(IOException.class), Collections.emptySet(), e -> false, URI.create("http://example.net"));

        Assert.assertEquals(retryHandler.runWithRetries(() -> 3), 3);
    }

    @Test
    public void test0MaxRetriesDoesntRetry() throws IOException {
        final RetryHandler retryHandler = new RetryHandler(0, Collections.emptySet(),
                Set.of(IOException.class), Collections.emptySet(), e -> false, URI.create("http://example.net"));

        AtomicInteger count = new AtomicInteger(0);

        try {
            retryHandler.runWithRetries(() -> {
                count.incrementAndGet();
                throw new IOException();
            });
        } catch (OutOfRetriesException e){
            Assert.assertEquals(e.getRetries(), 0);
            Assert.assertEquals(count.get(), 1);
        }
    }


}