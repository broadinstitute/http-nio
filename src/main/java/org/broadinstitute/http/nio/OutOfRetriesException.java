package org.broadinstitute.http.nio;

import java.io.IOException;
import java.time.Duration;

public class OutOfRetriesException extends IOException {
    private final int retries;
    private final Duration totalWaitTime;

    public OutOfRetriesException(int retries, Duration totalWaitTime, Throwable mostRecentFailureReason) {
        super("All %d retries failed. Waited a total of %d ms between attempts."
                .formatted(retries, totalWaitTime.toMillis()), mostRecentFailureReason);
        this.retries = retries;
        this.totalWaitTime = totalWaitTime;
    }

    public int getRetries() {
        return retries;
    }

    public Duration getTotalWaitTime() {
        return totalWaitTime;
    }
}
