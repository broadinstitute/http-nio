package org.broadinstitute.http.nio.utils;

import org.broadinstitute.http.nio.HttpFileSystemProviderSettings;
import org.broadinstitute.http.nio.RetryHandler;
import org.broadinstitute.http.nio.UnexpectedHttpResponseException;
import org.broadinstitute.http.nio.utils.ExceptionCauseIterator;
import org.broadinstitute.http.nio.utils.Utils;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.UnresolvedAddressException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;

/**
 * Utility class for working with HTTP/S connections and URLs.
 *
 * <p>Includes also constants for HTTP/S.
 *
 * @author Daniel Gomez-Sanchez (magicDGS)
 */
public final class HttpUtils {

    /** Separator {@code String} for path component of HTTP/S URL. */
    public static final String HTTP_PATH_SEPARATOR_STRING = "/";

    /** Separator {@code char} for path component of HTTP/S URL. */
    public static final char HTTP_PATH_SEPARATOR_CHAR = '/';

    /** Charset for path component of HTTP/S URL. */
    public static final Charset HTTP_PATH_CHARSET = StandardCharsets.UTF_8;

    // utility class - cannot be instantiated
    private HttpUtils() {}

    /**
     * Check if an {@link URI} exists.
     *
     * <p>A URI exists if the response code is 200 or 206
     * It does not exist of the response is 404 or an {@link UnresolvedAddressException} is thrown
     *
     * @param uri URI to test for existance.
     * @param settings the settings to use to build the http connections
     *
     * @return {@code true} if the URL exists; {@code false} otherwise.
     *
     * @throws IOException if an I/O error occurs.
     * @throws AccessDeniedException on http 401, 403, 407
     */
    public static boolean exists(final URI uri, HttpFileSystemProviderSettings settings) throws IOException {
        Utils.nonNull(uri, () -> "null uri");
        final HttpClient client = getClient(settings);
        final HttpRequest request = HttpRequest.newBuilder(uri)
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();

        final RetryHandler retryHandler = new RetryHandler(settings.retrySettings(), uri);
        return retryHandler.runWithRetries(() -> {
            try {
                final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                return switch (response.statusCode()) {
                    case 200, 206 -> true;
                    case 404 -> false; //doesn't exist
                    case 401 | 403 | 407 -> throw new AccessDeniedException("Access was denied to " + uri
                            + "\nHttp status: " + response.statusCode()
                            + "\n" + response.body());
                    default -> throw new UnexpectedHttpResponseException(response.statusCode(),
                            "Unexpected response from " + uri
                                    + "\nHttp status: " + response.statusCode()
                                    + "\n" + response.body());
                };
            } catch (ConnectException e){
                for(Throwable cause : new ExceptionCauseIterator(e)){
                    if(cause instanceof UnresolvedAddressException) {
                        return false;
                    }
                }
                throw e;
            } catch (InterruptedException e) {
                throw new InterruptedIOException("Connection thread was unexpectedly interrupted while checking existence of "+ uri +".");
            }
        });
    }

    /**
     * Get an HttpClient built wth appropriate settings.
     * @param settings the settings to use for the client
     * @return a new HttpClient
     */
    public static HttpClient getClient(final HttpFileSystemProviderSettings settings) {
        return HttpClient.newBuilder()
                .followRedirects(settings.redirect())
                .connectTimeout(settings.timeout())
                .build();
    }
}
