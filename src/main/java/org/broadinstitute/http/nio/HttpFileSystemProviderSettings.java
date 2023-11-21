package org.broadinstitute.http.nio;

import java.net.Proxy;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Collection;
import java.util.function.Predicate;

/**
 * Settings that control the behavior of newly instantiated Http(s)FileSystems
 * <p>
 * WARNING: These currently don't seem to be used anywhere...
 *
 * @param proxy      Proxy to use when making connections
 * @param useCaching should caching be enabled
 * @param timeout    timeout duration
 * @param retrySettings settings to control retry behavior
 */
public record HttpFileSystemProviderSettings(Proxy proxy, boolean useCaching,
                                             Duration timeout,
                                             HttpClient.Redirect redirect,
                                             RetrySettings retrySettings
                                           ) {

    public static final RetrySettings DEFAULT_RETRY_SETTINGS = new RetrySettings(3,
            HTTPRetryHandler.DEFAULT_RETRYABLE_HTTP_CODES,
            HTTPRetryHandler.DEFAULT_RETRYABLE_EXCEPTIONS,
            e -> false);

    /**
     * default settings which will be used unless they are reset
     */
    public static final HttpFileSystemProviderSettings DEFAULT_SETTINGS = new HttpFileSystemProviderSettings(null, false,
            Duration.ofSeconds(10), HttpClient.Redirect.NORMAL, DEFAULT_RETRY_SETTINGS );


    /**
     * Settings to control retry behavior
     * @param maxRetries number of times to retry an attempted network operation, must be >= 0
     * @param retryableHttpCodes a list of http response codes which will be retried when encountered
     * @param retryableExceptions a list of  exception classes which will be retried when encountered
     * @param retryPredicate an arbitrary predicate which allows handling retries in custom ways, applied after testing
     *                       response codes and retryable exceptions, if it returns true it will be retried
     *
     */
    public record RetrySettings(int maxRetries,
                                  Collection<Integer> retryableHttpCodes,
                                  Collection<Class<? extends Exception>> retryableExceptions,
                                  Predicate<Throwable> retryPredicate){}
}