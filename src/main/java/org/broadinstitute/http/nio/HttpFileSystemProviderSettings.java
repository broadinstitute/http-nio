package org.broadinstitute.http.nio;

import java.net.Proxy;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;

/**
 * Settings that control the behavior of newly instantiated Http(s)FileSystems
 *
 * WARNING: These currently don't seem to be used anywhere...
 * @param proxy Proxy to use when making connections
 * @param useCaching should caching be enabled
 * @param timeout timeout duration
 * @param maxRetries max number of retries before throwing an error
 */
public record HttpFileSystemProviderSettings(Proxy proxy, boolean useCaching,
                                             Duration timeout,
                                             HttpClient.Redirect redirect,
                                             int maxRetries,
                                             int maxReopens,
                                             Collection<Integer> retryableHttpCodes,
                                             Predicate<Exception> retryPredicate) {

    /**
     * default settings which will be used unless they are reset
     */
    public static final HttpFileSystemProviderSettings DEFAULT_SETTINGS = new HttpFileSystemProviderSettings(null, false, Duration.ofSeconds(10), HttpClient.Redirect.NORMAL, 3, 3, Collections.emptySet(), e -> false);

}