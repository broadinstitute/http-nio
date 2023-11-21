package org.broadinstitute.http.nio;

import javax.net.ssl.SSLException;
import java.io.EOFException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Simple counter class to keep track of retry and reopen attempts when StorageExceptions are
 * encountered. Handles sleeping between retry/reopen attempts, as well as throwing an exception
 * when all retries/reopens are exhausted.
 */
public class HTTPRetryHandler {
    public static final Set<Class<? extends Exception>> DEFAULT_RETRYABLE_EXCEPTIONS = Set.of(
            SSLException.class,
            EOFException.class,
            SocketException.class,
            SocketTimeoutException.class
    );

    public static final Set<Integer> DEFAULT_RETRYABLE_HTTP_CODES = Set.of(500, 502, 503);

    private Duration totalWaitTime = Duration.ZERO;
    private final int maxRetries;
    private final Set<Integer> retryableHttpCodes;
    private final Predicate<Throwable> customRetryPredicate;
    private final Set<Class<? extends Exception>> retryableExceptions;


    public HTTPRetryHandler(HttpFileSystemProviderSettings.RetrySettings settings) {
        this(settings.maxRetries(),
                settings.retryableHttpCodes(),
                settings.retryableExceptions(),
                settings.retryPredicate());
    }

    /**
     * Create a CloudStorageRetryHandler with the maximum retries and reopens set to different values.
     *
     * @param maxRetries         maximum number of retries
     * @param retryableHttpCodes HTTP codes that are retryable
     * @param retryPredicate     predicate to determine if an exception is retryable
     */
    public HTTPRetryHandler(
            final int maxRetries,
            final Collection<Integer> retryableHttpCodes,
            final Collection<Class<? extends Exception>> retryableExceptions,
            final Predicate<Throwable> retryPredicate) {
        this.maxRetries = maxRetries;
        this.retryableHttpCodes = Set.copyOf(retryableHttpCodes);
        this.retryableExceptions = Set.copyOf(retryableExceptions);
        this.customRetryPredicate = retryPredicate;
    }

    /**
     * @return the maximum allowed number of retries
     */
    public int getMaxRetries() {
        return maxRetries;
    }


    /**
     * @return the total time waited in this set of retries
     */
    public Duration getTotalWaitTime() {
        return totalWaitTime;
    }

    public void sleepBeforeNextAttempt(int attempt) {
        // exponential backoff, but let's bound it around 2min.
        // aggressive backoff because we're dealing with unusual cases.
        Duration delay = Duration.ofMillis((1L << Math.min(attempt, 7)));
        try {
            Thread.sleep(delay.toMillis());
            totalWaitTime = totalWaitTime.plus(delay);
        } catch (InterruptedException iex) {
            // reset interrupt flag
            Thread.currentThread().interrupt();
        }
    }

    /**
     * @param exs Exception to test
     * @return true if exs is a retryable error, otherwise false
     */
    public boolean isRetryable(final Exception exs) {
        // loop through all the causes in the exception chain in case it's buried
        for (Throwable cause : new ExceptionCauseIterator(exs)) {
            if (cause instanceof HttpSeekableByteChannel.UnexpectedHttpResponseException responseException) {
                return retryableHttpCodes.contains(responseException.getResponseCode());
            }

            for (Class<? extends Exception> retryable : retryableExceptions) {
                if (retryable.isInstance(cause)) {
                    return true;
                }
            }

            if(customRetryPredicate.test(cause)){
                return true;
            }
        }
        return false;
    }


}

