package org.broadinstitute.http.nio;

import java.net.Proxy;
import java.net.http.HttpClient;
import java.time.Duration;

/**
 * Settings that control the behavior of newly instantiated Http(s)FileSystems
 *
 * WARNING: These currently don't seem to be used anywhere...
 * @param proxy Proxy to use when making connections
 * @param useCaching should caching be enabled
 * @param timeout timeout duration
 * @param maxRetries max number of retries before throwing an error
 */
public record HttpFileSystemProviderSettings(Proxy proxy, boolean useCaching, Duration timeout, int maxRetries, HttpClient.Redirect redirect){
    /** default settings which will be used unless they are reset */
    public static final HttpFileSystemProviderSettings DEFAULT_SETTINGS = new HttpFileSystemProviderSettings(null, false, Duration.ofSeconds(10), 3, HttpClient.Redirect.NORMAL);
}
