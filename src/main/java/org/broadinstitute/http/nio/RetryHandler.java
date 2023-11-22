package org.broadinstitute.http.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Simple counter class to keep track of retry and reopen attempts when StorageExceptions are
 * encountered. Handles sleeping between retry/reopen attempts, as well as throwing an exception
 * when all retries/reopens are exhausted.
 */
public class RetryHandler {
    private static final Logger logger = LoggerFactory.getLogger(RetryHandler.class);
    public static final Set<Class<? extends Exception>> DEFAULT_RETRYABLE_EXCEPTIONS = Set.of(
            SSLException.class,
            EOFException.class,
            SocketException.class,
            SocketTimeoutException.class
    );

    public static final Set<Integer> DEFAULT_RETRYABLE_HTTP_CODES = Set.of(500, 502, 503);

    public int getMaxRetries() {
        return maxRetries;
    }

    private final int maxRetries;
    private final Set<Integer> retryableHttpCodes;
    private final Predicate<Throwable> customRetryPredicate;
    private final URI uri;
    private final Set<Class<? extends Exception>> retryableExceptions;


    public RetryHandler(HttpFileSystemProviderSettings.RetrySettings settings, URI uri) {
        this(settings.maxRetries(),
                settings.retryableHttpCodes(),
                settings.retryableExceptions(),
                settings.retryPredicate(),
                uri);

    }

    public interface IOSupplier<T> {
        T get() throws IOException;
    }

    public interface IORunnable {
        void run() throws IOException;
    }

    public void runWithRetries(final IORunnable toRun) throws IOException {
        runWithRetries((IOSupplier<Void>) (() -> {
            toRun.run();
            return null;
        }));
    }

    public <T> T runWithRetries(final IOSupplier<T> toRun) throws IOException {
        Duration totalSleepTime = Duration.ZERO;
        int tries = 0;
        IOException mostRecentFailureReason = null;
        while (tries <= maxRetries) {
            try {
                tries++;
                return toRun.get();
            } catch (final IOException ex) {
                mostRecentFailureReason = ex;

                if (isRetryable(ex)) {
                    //log a warning
                    logger.warn("Retrying connection to {} due to error: {}. " +
                            "\nThis will be retry #{}", uri, ex.getMessage(), tries);
                } else {
                    throw ex;
                }
            }
            totalSleepTime = totalSleepTime.plus(sleepBeforeNextAttempt(tries));
        }
        throw new OutOfRetriesException(tries-1, totalSleepTime, mostRecentFailureReason);
    }

    /**
     * Create a CloudStorageRetryHandler with the maximum retries and reopens set to different values.
     *
     * @param maxRetries         maximum number of retries
     * @param retryableHttpCodes HTTP codes that are retryable
     * @param retryPredicate     predicate to determine if an exception is retryable
     */
    public RetryHandler(
            final int maxRetries,
            final Collection<Integer> retryableHttpCodes,
            final Collection<Class<? extends Exception>> retryableExceptions,
            final Predicate<Throwable> retryPredicate,
            final URI uri) {
        Utils.validateArg(maxRetries >= 0, "retries must be >= 0, was " + maxRetries);
        this.maxRetries = maxRetries;
        this.retryableHttpCodes = Set.copyOf(Utils.nonNull(retryableHttpCodes, () -> "retryableHttpCodes"));
        this.retryableExceptions = Set.copyOf(Utils.nonNull(retryableExceptions, () -> "retryableExceptions"));
        this.customRetryPredicate = Utils.nonNull(retryPredicate, () -> "retryPredicate");
        this.uri = Utils.nonNull(uri, () -> "uri");
    }

    public Duration sleepBeforeNextAttempt(int attempt) {
        // exponential backoff, but let's bound it around 2min.
        // aggressive backoff because we're dealing with unusual cases.
        Duration delay = Duration.ofMillis((1L << Math.min(attempt, 7)));
        final Instant sleepStart = Instant.now();
        final Instant sleepEnd;
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException iex) {
            // reset interrupt flag
            Thread.currentThread().interrupt();
        } finally {
            sleepEnd = Instant.now();
        }
        return Duration.between(sleepStart, sleepEnd);
    }

    /**
     * @param exs Exception to test
     * @return true if exs is a retryable error, otherwise false
     */
    public boolean isRetryable(final Exception exs) {
        // loop through all the causes in the exception chain in case it's buried
        for (Throwable cause : new ExceptionCauseIterator(exs)) {
            if (cause instanceof HttpSeekableByteChannel.UnexpectedHttpResponseException responseException) {
                if( retryableHttpCodes.contains(responseException.getResponseCode())){
                    //give the custom predicate a chance to handle unknown response codes by only returning when true
                    return true;
                }
            }

            for (Class<? extends Exception> retryable : retryableExceptions) {
                if (retryable.isInstance(cause)) {
                    return true;
                }
            }

            if (customRetryPredicate.test(cause)) {
                return true;
            }
        }
        return false;
    }


}
