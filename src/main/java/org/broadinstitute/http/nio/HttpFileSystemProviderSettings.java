package org.broadinstitute.http.nio;

import org.broadinstitute.http.nio.utils.Utils;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Collection;
import java.util.function.Predicate;

/**
 * Settings that control the behavior of newly instantiated Http(s)FileSystems
 */
public record HttpFileSystemProviderSettings(Duration timeout,
                                             HttpClient.Redirect redirect,
                                             RetrySettings retrySettings
                                           ) {

    /**
     * @param timeout   the timeout to use when waiting on http connections
     * @param redirect  should redirects be followed automatically
     * @param retrySettings settings which control how retries are handled
     */
    public HttpFileSystemProviderSettings {
        Utils.nonNull(timeout, () -> "timeout");
        Utils.nonNull(redirect, () -> "redirect");
        Utils.nonNull(retrySettings, () -> "retrySettings");
    }

    /**
     * The default retry settings, these are a good basis for retry handling
     */
    public static final RetrySettings DEFAULT_RETRY_SETTINGS = new RetrySettings(3,
            RetryHandler.DEFAULT_RETRYABLE_HTTP_CODES,
            RetryHandler.DEFAULT_RETRYABLE_EXCEPTIONS,
            RetryHandler.DEFALT_RETRYABLE_MESSAGES,
            e -> false);

    /**
     * default settings which will be used unless they are reset
     */
    public static final HttpFileSystemProviderSettings DEFAULT_SETTINGS = new HttpFileSystemProviderSettings(
            Duration.ofSeconds(10), HttpClient.Redirect.NORMAL, DEFAULT_RETRY_SETTINGS );


    /**
     * Settings which control the behavior of http retries
     */
    public record RetrySettings(int maxRetries,
                                  Collection<Integer> retryableHttpCodes,
                                  Collection<Class<? extends Exception>> retryableExceptions,
                                  Collection<String> retryableMessages,
                                  Predicate<Throwable> retryPredicate){

        /**
         * Settings to control retry behavior
         * @param maxRetries number of times to retry an attempted network operation, must be >= 0
         * @param retryableHttpCodes a list of http response codes which will be retried when encountered
         * @param retryableExceptions a list of  exception classes which will be retried when encountered
         * @param retryableMessages a list of messages which will be retried when found in an exception message
         * @param retryPredicate an arbitrary predicate which allows handling retries in custom ways, applied after testing
         *                       response codes and retryable exceptions, if it returns true it will be retried
         *
         */
        public RetrySettings {
            Utils.validateArg( maxRetries >= 0, "maxRetries must be >= 0");
            Utils.nonNull(retryableHttpCodes, () -> "retryableHttpCodes");
            Utils.nonNull(retryableExceptions, () -> "retryableExceptions");
            Utils.nonNull(retryableMessages,() -> "retryableMessages");
            Utils.nonNull(retryPredicate, () -> "retryPredicate");
        }
    }
}