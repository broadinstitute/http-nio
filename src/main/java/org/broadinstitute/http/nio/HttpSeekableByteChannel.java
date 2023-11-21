package org.broadinstitute.http.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.Map;


/**
 * Implementation for a {@link SeekableByteChannel} for {@link URL} open as a connection.
 *
 * <p>The current implementation is thread-safe using the {@code synchronized} keyword in every
 * method.
 *
 * @author Daniel Gomez-Sanchez (magicDGS)
 * @implNote this seekable byte channel is read-only.
 */
public class HttpSeekableByteChannel implements SeekableByteChannel {

    private static final long SKIP_DISTANCE = 8 * 1024;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    // url and proxy for the file
    private final URI uri;

    // current position of the SeekableByteChannel
    private long position = 0;

    // the size of the whole file (-1 is not initialized)
    private long size = -1;

    private final HttpClient client;
    private ReadableByteChannel channel = null;
    private InputStream backingStream = null;

    private HttpFileSystemProviderSettings settings = null;


    private static HttpClient getDefaultClient() {
        return HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    //useful for testing, not generally used
    HttpSeekableByteChannel(URI uri) throws IOException {
        this(uri, getDefaultClient(), HttpFileSystemProviderSettings.DEFAULT_SETTINGS);
    }

    public HttpSeekableByteChannel(final URI uri, final HttpClient client, HttpFileSystemProviderSettings settings) throws IOException {
        this.uri = Utils.nonNull(uri, () -> "null URI");
        this.client = client;
        this.settings = settings;

        // and instantiate the stream/channel at position 0
        instantiateChannel(0L);

    }


    @Override
    public synchronized int read(final ByteBuffer dst) throws IOException {
        return runWithRetries( () -> {
                final int read = channel.read(dst);
                if (read != -1) {
                    this.position += read;
                }
                return read;
        });
    }

    @Override
    public int write(ByteBuffer src) {
        throw new NonWritableChannelException();
    }

    @Override
    public synchronized long position() throws IOException {
        if (!isOpen()) {
            throw new ClosedChannelException();
        }
        return position;
    }

    @Override
    public synchronized HttpSeekableByteChannel position(long newPosition) throws IOException {
        if (newPosition < 0) {
            throw new IllegalArgumentException("Cannot seek to a negative position (from " + position + " to " + newPosition + " ).");
        }
        if (!isOpen()) {
            throw new ClosedChannelException();
        }
        if (this.position == newPosition) {
            //nothing to do here
        } else if (this.position < newPosition && newPosition - this.position < SKIP_DISTANCE) {
            //TODO this case is special since it messes up the state of the stream if it fails.
            try {
                // if the current position is before but nearby do not open a new connection
                // but skip the bytes until the new position
                long bytesToSkip = newPosition - this.position;
                while (bytesToSkip > 0) {
                    final long skipped = backingStream.skip(bytesToSkip);
                    if (skipped <= 0) {
                        throw new IOException("Failed to skip any bytes while moving from " + this.position + " to " + newPosition);
                    }
                    bytesToSkip -= skipped;
                }
                logger.debug("Skipped {} bytes out of {} for setting position to {} (previously on {})",
                        bytesToSkip, bytesToSkip, newPosition, position);
            } catch (IOException ex){
                //If we encounter a problem just reopen and use those retries.
                closeSilently();
                instantiateChannel(newPosition);
            }
        } else {
            // in this case, we require to re-instantiate the channel
            // opening at the new position - and closing the previous
            closeSilently();
            instantiateChannel(newPosition);
        }

        // update to the new position
        this.position = newPosition;

        return this;
    }

    @Override
    public synchronized long size() throws IOException {
        runWithRetries( () -> {
            if (!isOpen()) {
                throw new ClosedChannelException();
            }
            if (size == -1) {
                HttpRequest headRequest = HttpRequest.newBuilder()
                        .uri(uri)
                        .method("HEAD", HttpRequest.BodyPublishers.noBody())
                        .build();
                try {
                    final HttpResponse<?> response = client.send(headRequest, HttpResponse.BodyHandlers.discarding());
                    assertGoodHttpResponse(response, false);
                    final Map<String, List<String>> map = response.headers().map();
                    final List<String> contentLengthStrings = map.get("content-length");
                    if (contentLengthStrings == null || contentLengthStrings.size() != 1) {
                        throw new IOException("Failed to get size of file at " + uri.toString() + "," +
                                " content-length=" + contentLengthStrings);
                    } else {
                        size = Long.parseLong(contentLengthStrings.get(0));
                    }
                } catch (InterruptedException e) {
                    throw new IOException("Interrupted while trying to get size of file at " + uri.toString(), e);
                }
            }
        });
        return size;
    }

    /**
     * Checks for the approprate http response code and throws an exception if the wrong one is found.
     * @param response the completed HttpResponse
     * @param isRangeRequest if this query was expecting a subrange of the file
     * @throws FileNotFoundException on a 404
     * @throws IncompatibleResponseToRangeQueryException if it expected a subset of the file but got the whole thing or vice versa
     * @throws UnexpectedHttpResponseException if it reieves any other http response
     */
    private void assertGoodHttpResponse(final HttpResponse<?> response, boolean isRangeRequest) throws FileNotFoundException, UnexpectedHttpResponseException {
        int code = response.statusCode();
        switch (code) {
            case 200 -> {
                if (isRangeRequest) {
                    throw new IncompatibleResponseToRangeQueryException(200, "Server returned entire file instead of subrange for " + uri);
                }
            }
            case 206 -> {
                if (!isRangeRequest) {
                    throw new IncompatibleResponseToRangeQueryException(206, "Unexpected Partial Content result for request for entire file at " + uri);
                }
            }
            case 404 -> throw new FileNotFoundException("File not found at " + uri + " got http 404 response.");
            default -> throw new UnexpectedHttpResponseException(code, "Unexpected http response code: " + code + " when requesting " + uri);
        }
    }


    @Override
    public SeekableByteChannel truncate(long size) {
        throw new NonWritableChannelException();
    }

    @Override
    public synchronized boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public synchronized void close() throws IOException {
        // this also closes the backing stream
        channel.close();
    }

    private synchronized void closeSilently(){
        try {
            close();
        } catch (IOException e) {
            // swallow this
        }
    }

    // open a readable byte channel for the requested position
    private synchronized void instantiateChannel(final long position) throws IOException {
        runWithRetries(() -> {
            final HttpRequest.Builder builder = HttpRequest.newBuilder(uri).GET();
            final boolean isRangeRequest = position != 0;
            if (isRangeRequest) {
                builder.setHeader("Range", "bytes=" + position + "-");
            }
            HttpRequest request = builder.build();

            final HttpResponse<InputStream> response;
            try {
                response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
            } catch (final FileNotFoundException ex) {
                throw ex;
            } catch (final IOException ex) {
                throw new IOException("Failed to connect to " + uri + "at positon: " + position, ex);
            } catch (final InterruptedException ex) {
                throw new IOException("Interrupted while connecting to " + uri + " at position: " + position, ex);
            }
            assertGoodHttpResponse(response, isRangeRequest);
            backingStream = new BufferedInputStream(response.body());
            channel = Channels.newChannel(backingStream);
            this.position = position;
        });
    }

    private interface IOSupplier<T> {
        public T get() throws IOException;
    }

    private interface IORunnable {
        public void run() throws IOException;
    }

    private void runWithRetries(final IORunnable toRun) throws IOException {
        runWithRetries((IOSupplier<Void>)(() -> {toRun.run(); return null;}));
    }

    public static class UnexpectedHttpResponseException extends IOException {
        private final int responseCode;

        public UnexpectedHttpResponseException(int responseCode, String msg){
            super(msg);
            this.responseCode = responseCode;
        }

        public int getResponseCode() {
            return responseCode;
        }
    }

    public static class IncompatibleResponseToRangeQueryException extends UnexpectedHttpResponseException {
        public IncompatibleResponseToRangeQueryException(int code, String msg){
            super(code, msg);
        }
    }

    private <T> T runWithRetries(final IOSupplier<T> toRun) throws IOException {
        final HTTPRetryHandler retryHandler = new HTTPRetryHandler(settings.retrySettings());

        int retries = 0;
        IOException mostRecentFailureReason = null;
        while(retries <= retryHandler.getMaxRetries()) {
            try {
                return toRun.get();
            } catch (final IOException ex) {
                mostRecentFailureReason = ex;

                if (retryHandler.isRetryable(ex)) {
                    retries++;
                    //log a warning
                    logger.warn("Retrying connection to {} at position {} due to error: {}. \nThis will be retry #{}", uri, position, ex.getMessage(), retries);
                } else {
                    throw ex;
                }
            }
            retryHandler.sleepBeforeNextAttempt(retries);
        }
        throw new IOException(
                "All %d retries failed. Waited a total of %d ms between attempts."
                        .formatted(retries, retryHandler.getTotalWaitTime().toMillis()),
                mostRecentFailureReason);
    }
}
