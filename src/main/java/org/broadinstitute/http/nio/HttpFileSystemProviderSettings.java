package org.broadinstitute.http.nio;

import java.net.Proxy;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;

public class HttpFileSystemProviderSettings {

    public static final HttpFileSystemProviderSettings DEFAULT_SETTINGS = new HttpFileSystemProviderSettings(null, false, 10, 3, 3, Collections.emptyList(), e -> false);

    private final Proxy proxy;
    private final boolean useCaching;


    private final int timeout;
    private final int maxRetries;
    private final int maxReopens;
    private final Collection<Integer> retryableHttpCodes;
    private final Predicate<Exception> retryPredicate;


    public HttpFileSystemProviderSettings(final Proxy proxy, final boolean useCaching, final int timeout, final int maxRetries, final int maxReopens, final Collection<Integer> retryableHttpCodes, final Predicate<Exception> retryPredicate) {
        this.proxy = proxy;
        this.useCaching = useCaching;
        this.timeout = timeout;
        this.maxRetries = maxRetries;
        this.maxReopens = maxReopens;
        this.retryableHttpCodes = retryableHttpCodes;
        this.retryPredicate = retryPredicate;
    }

    public Proxy getProxy() {
        return proxy;
    }

    public boolean isCachingUsed() {
        return useCaching;
    }

    public int getTimeout() {
        return timeout;
    }


    public int getMaxRetries() {
        return maxRetries;
    }

    public int getMaxReopens() {
        return maxReopens;
    }

    public Collection<Integer> getRetryableHttpCodes() {
        return retryableHttpCodes;
    }

    public Predicate<Exception> getRetryPredicate() {
        return retryPredicate;
    }
}
