package org.broadinstitute.http.nio;

import javax.net.ssl.HttpsURLConnection;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLConnection;
import java.util.Collection;
import java.util.function.Predicate;

/**
 * Simple counter class to keep track of retry and reopen attempts when StorageExceptions are
 * encountered. Handles sleeping between retry/reopen attempts, as well as throwing an exception
 * when all retries/reopens are exhausted.
 */
public class HTTPRetryHandler {
    private int retries;
    private int reopens;
    private long totalWaitTime; // in milliseconds
    private final int maxRetries;
    private final int maxReopens;
    private final Collection<Integer> retryableHttpCodes;
    private final Predicate<Exception> retryPredicate;

    /**
     * Create a CloudStorageRetryHandler with global static config values.
     */
    public HTTPRetryHandler(HttpFileSystemProviderSettings settings) {
        this(
                settings.getMaxRetries(),
                settings.getMaxReopens(),
                settings.getTimeout(),
                settings.getRetryableHttpCodes(),
                settings.getRetryPredicate());
    }

    /**
     * Create a CloudStorageRetryHandler with the maximum retries and reopens set to different values.
     *
     * @param maxRetries maximum number of retries
     * @param maxReopens maximum number of reopens
     * @param totalWaitTime total wait time in milliseconds)
     * @param retryableHttpCodes HTTP codes that are retryable
     * @param retryPredicate predicate to determine if an exception is retryable
     */
    public HTTPRetryHandler(
            final int maxRetries, final int maxReopens, final long totalWaitTime,
            final Collection<Integer> retryableHttpCodes, final Predicate<Exception> retryPredicate) {
        this.maxRetries = maxRetries;
        this.maxReopens = maxReopens;
        this.totalWaitTime = totalWaitTime;
        this.retryableHttpCodes = retryableHttpCodes;
        this.retryPredicate = retryPredicate;
    }

    /** @return number of retries we've performed */
    public int retries() {
        return retries;
    }

    /** @return number of reopens we've performed */
    public int reopens() {
        return reopens;
    }

    /**
     * Checks whether we should retry, reopen, or give up.
     *
     * <p>In the latter case it throws an exception (this includes the scenario where we exhausted the
     * retry count).
     *
     * <p>Otherwise, it sleeps for a bit and returns whether we should reopen. The sleep time is
     * dependent on the retry number.
     *
     * @param exs caught StorageException
     * @return True if its re retryable.
     * @throws IOException if the exception is not retryable, or if you ran out of retries.
     */
    public boolean handleStorageException(final IOException exs, final URLConnection connection) throws IOException {
        // None of the retryable exceptions are reopenable, so it's OK to write the code this way.
        if (isRetryable(exs, connection)) {
            handleRetryForException(exs, connection);
            return true;
        }
        throw exs;
    }

    /**
     * Records a retry attempt for the given StorageException, sleeping for an amount of time
     * dependent on the attempt number. Throws a StorageException if we've exhausted all retries.
     *
     * @param exs The StorageException error that prompted this retry attempt.
     */
    private void handleRetryForException(final Exception exs, final URLConnection connection) throws IOException {
        retries++;
        if (retries > maxRetries) {
            throw new IOException(
                    "HTTP Response Code: "+getResponseCodeForConnection(connection)+" All "
                            + maxRetries
                            + " retries failed. Waited a total of "
                            + totalWaitTime
                            + " ms between attempts",
                    exs);
        }
        sleepForAttempt(retries);
    }
//    /**
//     * Records a reopen attempt for the given StorageException, sleeping for an amount of time
//     * dependent on the attempt number. Throws a StorageException if we've exhausted all reopens.
//     *
//     * @param exs The StorageException error that prompted this reopen attempt.
//     */
//    private void handleReopenForStorageException(final StorageException exs) throws StorageException {
//        reopens++;
//        if (reopens > maxReopens) {
//            throw new StorageException(
//                    exs.getCode(),
//                    "All "
//                            + maxReopens
//                            + " reopens failed. Waited a total of "
//                            + totalWaitTime
//                            + " ms between attempts",
//                    exs);
//        }
//        sleepForAttempt(reopens);
//    }

    void sleepForAttempt(int attempt) {
        // exponential backoff, but let's bound it around 2min.
        // aggressive backoff because we're dealing with unusual cases.
        long delay = 1000L * (1L << Math.min(attempt, 7));
        try {
            Thread.sleep(delay);
            totalWaitTime += delay;
        } catch (InterruptedException iex) {
            // reset interrupt flag
            Thread.currentThread().interrupt();
        }
    }

    /**
     * @param exs Exception to test
     * @return true if exs is a retryable error, otherwise false
     */
    boolean isRetryable(final Exception exs, final URLConnection connection) throws IOException {

        if (retryPredicate.test(exs)) {
            return true;
        }
        if (exs instanceof IOException) {
            // Currently the only connections we will ever attempt to open using this object are HTTP and HTTPS URI connetions, thus this is a vild cast
            int responsecode = getResponseCodeForConnection(connection);
            if( retryableHttpCodes.contains(responsecode)) {
                return true;
            }
        }
        return false;
    }

    private int getResponseCodeForConnection(URLConnection connection) throws IOException {
        try {
            final String statusLine = ((HttpURLConnection) connection).getHeaderField(0);
            int codePos = statusLine.indexOf(' ');
            if (codePos > 0) {

                int phrasePos = statusLine.indexOf(' ', codePos + 1);

                // deviation from RFC 2616 - don't reject status line
                // if SP Reason-Phrase is not included.
                if (phrasePos < 0)
                    phrasePos = statusLine.length();

                int responseCode = Integer.parseInt
                        (statusLine.substring(codePos + 1, phrasePos));
                return responseCode;
            } else {
                throw new IOException("Could not extract HTTP status from header");
            }

        } catch (NumberFormatException e) {
            throw new IOException("Invalid statusLine: " + ((HttpURLConnection) connection).getHeaderField(0), e);
        }
    }
//
//    /**
//     * @param exs StorageException to test
//     * @return true if exs is an error that can be resolved via a channel reopen, otherwise false
//     */
//    @VisibleForTesting
//    boolean isReopenable(final StorageException exs) {
//        Throwable throwable = exs;
//        // ensures finite iteration
//        int maxDepth = 20;
//        while (throwable != null && maxDepth-- > 0) {
//            for (Class<? extends Exception> reopenableException : config.reopenableExceptions()) {
//                if (reopenableException.isInstance(throwable)) {
//                    return true;
//                }
//            }
//            if (throwable.getMessage() != null
//                    && throwable.getMessage().contains("Connection closed prematurely")) {
//                return true;
//            }
//            throwable = throwable.getCause();
//        }
//        return false;
//    }
}