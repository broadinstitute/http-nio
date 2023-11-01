package org.broadinstitute.http.nio;

import java.net.Proxy;

/**
 * Settings that control the behavior of newly instantiated Http(s)FileSystems
 *
 * WARNING: These currently don't seem to be used anywhere...
 * @param proxy Proxy to use when making connections
 * @param useCaching should caching be enabled
 * @param timeout timeout in seconds
 * @param maxRetries max number of retries before throwing an error
 */
public record HttpFileSystemProviderSettings( Proxy proxy, boolean useCaching, int timeout, int maxRetries ){
    /** default settings which will be used unless they are reset */
    public static final HttpFileSystemProviderSettings DEFAULT_SETTINGS = new HttpFileSystemProviderSettings(null, false, 10, 3);
}
