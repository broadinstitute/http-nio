package org.broadinstitute.http.nio;

import java.io.IOException;
import java.time.Duration;

/**
 * Indicates a failure which could be retried but was not because all allowed retries were exhausted.
 */
public class OutOfRetriesException extends IOException {

    /** number of retries attempted before giving up */
    private final int retries;

    /** total wait time between retries */
    private final Duration totalWaitTime;

    /**
     * @param retries the number of times the error was retried
     * @param totalWaitTime how long we waited between all the given retries
     * @param mostRecentFailureReason the most recently thrown exception
     */
    public OutOfRetriesException(int retries, Duration totalWaitTime, Throwable mostRecentFailureReason) {
        super("All %d retries failed. Waited a total of %d ms between attempts."
                .formatted(retries, totalWaitTime.toMillis()), mostRecentFailureReason);
        this.retries = retries;
        this.totalWaitTime = totalWaitTime;
    }

    /**
     * @return the number of times this was retried before giving up
     */
    public int getRetries() {
        return retries;
    }

    /**
     * @return the total amount of wait time between retries before giving up
     */
    public Duration getTotalWaitTime() {
        return totalWaitTime;
    }
}
