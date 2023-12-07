package org.broadinstitute.http.nio;

import org.broadinstitute.http.nio.utils.ExceptionCauseIterator;
import org.broadinstitute.http.nio.utils.Utils;
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
import java.util.function.Supplier;

/**
 * Simple counter class to keep track of retry and reopen attempts when StorageExceptions are
 * encountered. Handles sleeping between retry/reopen attempts, as well as throwing an exception
 * when all retries/reopens are exhausted.
 *
 * methods may be run and retried by running them through one of the {@link #runWithRetries(IORunnable)} methods
 */
public class RetryHandler {

    /**
     * the default set of exception messages which are retried when encountered
     */
    public static final Set<String> DEFALT_RETRYABLE_MESSAGES = Set.of("protocol error:");
    //IOExceptions with the string `protocol error` can happen when there is bad data returned during an http request

    /**
     * default set of exception types which will be retried when encountered
     */
    public static final Set<Class<? extends Exception>> DEFAULT_RETRYABLE_EXCEPTIONS = Set.of(
            SSLException.class,
            EOFException.class,
            SocketException.class,
            SocketTimeoutException.class
    );

    /**
     * default set of HTTP codes which will be retried
     */
    public static final Set<Integer> DEFAULT_RETRYABLE_HTTP_CODES = Set.of(500, 502, 503);

    private static final Logger LOGGER = LoggerFactory.getLogger(RetryHandler.class);


    /**
     * @return the maximum number of retries before giving up
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    private final int maxRetries;
    private final Set<Integer> retryableHttpCodes;
    private final Set<String> retryableMessages;
    private final Set<Class<? extends Exception>> retryableExceptions;
    private final Predicate<Throwable> customRetryPredicate;
    private final URI uri;

    /**
     * @param settings to configure the retry mechanism
     * @param uri which URI is being queried, used in error messages
     */
    public RetryHandler(HttpFileSystemProviderSettings.RetrySettings settings, URI uri) {
        this(settings.maxRetries(),
                settings.retryableHttpCodes(),
                settings.retryableExceptions(),
                settings.retryableMessages(),
                settings.retryPredicate(),
                uri);
    }

    /**
     * Create a CloudStorageRetryHandler with the maximum retries and reopens set to different values.
     *
     * @param maxRetries          maximum number of retries, 0 means nothing will be retried
     * @param retryableHttpCodes  HTTP codes that are retryable
     * @param retryableExceptions exception classes to retry when encountered
     * @param retryableMessages   strings which will be matched against the exception messages
     * @param retryPredicate      predicate to determine if an exception is retryable
     * @param uri                 URI which is being retried, used in the error messages
     */
    public RetryHandler(
            final int maxRetries,
            final Collection<Integer> retryableHttpCodes,
            final Collection<Class<? extends Exception>> retryableExceptions,
            final Collection<String> retryableMessages,
            final Predicate<Throwable> retryPredicate,
            final URI uri) {
        Utils.validateArg(maxRetries >= 0, "retries must be >= 0, was " + maxRetries);
        this.maxRetries = maxRetries;
        this.retryableHttpCodes = Set.copyOf(Utils.nonNull(retryableHttpCodes, () -> "retryableHttpCodes"));
        this.retryableExceptions = Set.copyOf(Utils.nonNull(retryableExceptions, () -> "retryableExceptions"));
        this.retryableMessages = Set.copyOf(Utils.nonNull(retryableMessages, () -> "retryableMessages"));
        this.customRetryPredicate = Utils.nonNull(retryPredicate, () -> "retryPredicate");
        this.uri = Utils.nonNull(uri, () -> "uri");
    }

    /**
     * {@linkplain Supplier} equivalent which can throw IOException
     * @param <T> supplied type
     */
    @FunctionalInterface
    public interface IOSupplier<T> {

        /**
         * Equivalent to Supplier.get()
         * @return the value returned
         * @throws IOException if there is an error during operation
         */
        T get() throws IOException;
    }

    /**
     * {@linkplain } equivalent which can throw IOException
     */
    @FunctionalInterface
    public interface IORunnable {

        /**
         * equivalent to Runnable.run()
         * @throws IOException if there is an error during operation
         */
        void run() throws IOException;
    }

    /**
     * A function to run and potentially retry if an error occurs and meets the retry criteria
     * Note that functions may be run repeatedly so any state which is changed during an unsuccessful attempt must
     * not poison the class.  Functions must either clean up in a finally block or reset state entirely each time.
     * @param toRun the function to run
     * @throws IOException when the function throws an IOException and either it is unretryable or retries are exhausted
     *         in the case of a retryable error which is not retried this will be an {@link OutOfRetriesException}
     */
    public void runWithRetries(final IORunnable toRun) throws IOException {
        runWithRetries((IOSupplier<Void>) (() -> {
            toRun.run();
            return null;
        }));
    }

    /**
     * A function to run and potentially retry if an error occurs and meets the retry criteria
     * Note that functions may be run repeatedly so any state which is changed during an unsuccessful attempt must
     * not poison the class.  Functions must either clean up in a finally block or reset state entirely each time.
     * @param  toRun the function to run
     * @param  <T> the type of the value returned by toRun
     * @throws IOException when the function throws an IOException and either it is unretryable or retries are exhausted
     *         in the case of a retryable error which is not retried this will be an {@link OutOfRetriesException}
     * @return the value supplied by succesful completion of toRun
     */
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
                    LOGGER.warn("Retrying connection to {} due to error: {}. " +
                            "\nThis will be retry #{}", uri, ex.getMessage(), tries);
                } else {
                    throw ex;
                }
            }
            totalSleepTime = totalSleepTime.plus(sleepBeforeNextAttempt(tries));
        }
        throw new OutOfRetriesException(tries - 1, totalSleepTime, mostRecentFailureReason);
    }

    /**
     * @param attempt attempt number, used to determine the wait time
     * @return the actual amount of time this slept for
     */
    private static Duration sleepBeforeNextAttempt(int attempt) {
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
            if (cause instanceof UnexpectedHttpResponseException responseException) {
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

            for (String message : retryableMessages) {
                final String errorMessage = cause.getMessage();
                if (errorMessage != null && errorMessage.contains(message)){
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
