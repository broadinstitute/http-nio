package org.broadinstitute.http.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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


    // request 'HEAD' method
    private static final String HEAD_REQUEST_METHOD = "HEAD";
    // key for 'Range' request
    private static final String RANGE_REQUEST_PROPERTY_KEY = "Range";
    // value for 'Range' request: START + POSITION + SEPARATOR (+ END)
    private static final String RANGE_REQUEST_PROPERTY_VALUE_START = "bytes=";
    private static final String RANGE_REQUEST_PROPERTY_VALUE_SEPARATOR = "-";

    // logger for HttpUtils
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtils.class);

    // utility class - cannot be instantiated
    private HttpUtils() {}

    /**
     * Disconnects the {@link URLConnection} if it is an instance of {@link HttpURLConnection}.
     *
     * @param connection the connection to be disconnected.
     */
    public static void disconnect(final URLConnection connection) {
        Utils.nonNull(connection, () -> "null URL connection");
        if (connection instanceof HttpURLConnection) {
            ((HttpURLConnection) connection).disconnect();
        }
    }

    /**
     * Check if an {@link URL} exists.
     *
     * <p>An URL exists if:
     *
     * <ul>
     * <li>The response code returned is {@link HttpURLConnection#HTTP_OK}.</li>
     * <li>The host is known (connection does not throw {@link UnknownHostException}.</li>
     * </ul>
     *
     * @param url URL to test for existance.
     *
     * @return {@code true} if the URL exists; {@code false} otherwise.
     *
     * @throws IOException if an I/O error occurs.
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
                    default -> throw new HttpSeekableByteChannel.UnexpectedHttpResponseException(response.statusCode(),
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
                throw new RuntimeException("Connection thread was unexpectedly interrupted while checking existence of "+ uri +".", e);
            }
        });
    }
    /**
     * Request a range of bytes for a {@link URLConnection}.
     *
     * @param connection the connection to request the range.
     * @param start      positive byte number to start the request.
     * @param end        positive byte number to end the request; {@code -1} if no bounded.
     *
     * @throws IllegalStateException    if the connection is already connected.
     * @throws IllegalArgumentException if the request is invalid.
     */
    public static void setRangeRequest(final URLConnection connection, final long start,
            final long end) {
        Utils.nonNull(connection, () -> "Null URLConnection");
        // setting the request range
        String request = RANGE_REQUEST_PROPERTY_VALUE_START
                + start
                + RANGE_REQUEST_PROPERTY_VALUE_SEPARATOR;
        // include end bound
        if (end != -1) {
            request += end;
        }

        // invalid request params should throw
        if (start < 0 || end < -1 || (end != -1 && end < start)) {
            throw new IllegalArgumentException("Invalid request: " + request);
        }

        LOGGER.debug("Request '{}' {} for {}", RANGE_REQUEST_PROPERTY_KEY, request, connection);
        // set the range if the position is different from 0
        connection.setRequestProperty(RANGE_REQUEST_PROPERTY_KEY, request);
    }

    static HttpClient getClient(final HttpFileSystemProviderSettings settings) {
        return HttpClient.newBuilder()
                .followRedirects(settings.redirect())
                .connectTimeout(settings.timeout())
                .build();
    }
}
