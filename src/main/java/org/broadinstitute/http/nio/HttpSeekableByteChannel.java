package org.broadinstitute.http.nio;

import org.broadinstitute.http.nio.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
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
 * @implNote this seekabe byte channel is read-only.
 */
public class HttpSeekableByteChannel implements SeekableByteChannel {

    private static final long SKIP_DISTANCE = 8 * 1024;
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSeekableByteChannel.class);

    // url and proxy for the file
    private final URI uri;
    private final RetryHandler retryHandler;

    private final HttpClient client;
    private ReadableByteChannel channel = null;
    private InputStream backingStream = null;

    // current position of the SeekableByteChannel
    private long position = 0;

    // the size of the whole file (-1 is not initialized)
    private long size = -1;


    /**
     * create a new seekable channel with default setttings at beggining of the file
     * @param uri the URI to connect to, this should not include range parameters already
     * @throws IOException if no connection can be established
     */
    public HttpSeekableByteChannel(URI uri) throws IOException {
        this(uri, HttpFileSystemProviderSettings.DEFAULT_SETTINGS, 0L);
    }

    /**
     * Create a new seekable channel with default setttins and seek to the given position
     * @param uri the URI to connect to, this should not include range parameters already
     * @param position an initial byte offset to open the file at
     * @throws IOException if no connection can be established
     */
    public HttpSeekableByteChannel(URI uri, long position) throws IOException {
        this(uri, HttpFileSystemProviderSettings.DEFAULT_SETTINGS, position);
    }

    /**
     * Create a new seekable channel which reads from the requested URI
     * @param uri the URI to connect to, this should not include range parameters already
     * @param settings settings to configure the connection and retry handling
     * @param position an initial byte offset to open the file at
     * @throws IOException if no connection can be established
     */
    public HttpSeekableByteChannel(final URI uri, HttpFileSystemProviderSettings settings, final long position) throws IOException {
        this.uri = Utils.nonNull(uri, () -> "null URI");
        this.client = HttpUtils.getClient(Utils.nonNull(settings, () -> "settings"));
        this.retryHandler = new RetryHandler(settings.retrySettings(), uri);
        // and instantiate the stream/channel
        retryHandler.runWithRetries(() -> openChannel(position));
    }

    @Override
    public synchronized int read(final ByteBuffer dst) throws IOException {
        assertChannelIsOpen();
        final int read = retryHandler.tryOnceThenWithRetries(
                () -> readWithoutPerturbingTheBufferIfAnErrorOccurs(dst, channel),
                () -> {
                    // a failed read will leave the underlying channel in an indeterminate state so we have to reopen it
                    closeSilently();
                    openChannel(position);
                    return readWithoutPerturbingTheBufferIfAnErrorOccurs(dst, channel);
                });

        if (read != -1) {
            this.position += read;
        }
        return read;
    }

    /**
     * Performs the equivalent of a channel.read(buf) operation but in the case of an exception the state of the input
     * buffer is not adversely impacted.
     *
     * @param dst a ByteBuffer to read into
     * @param channel the channel to reaad from
     * @return the number of bytes read from the channel
     * @throws IOException if the read operation throws
     */
    public static int readWithoutPerturbingTheBufferIfAnErrorOccurs(final ByteBuffer dst, final ReadableByteChannel channel) throws IOException {
        //create a view of the buffer
        final ByteBuffer copy = dst.duplicate();
        copy.order(dst.order());
        
        //this could fail
        final int read = channel.read(copy);

        //on success, we update the original to the new position in the view
        dst.position(copy.position());
        return read;
    }

    private void assertChannelIsOpen() throws ClosedChannelException {
        if(!isOpen()){
            throw new ClosedChannelException();
        }
    }

    @Override
    public int write(ByteBuffer src) {
        throw new NonWritableChannelException();
    }

    @Override
    public synchronized long position() throws IOException {
        assertChannelIsOpen();
        return position;
    }

    @Override
    public synchronized HttpSeekableByteChannel position(long newPosition) throws IOException {
        assertChannelIsOpen();
        Utils.validateArg(newPosition >= 0, "Cannot seek to a negative position (from " + position + " to " + newPosition + " ).");
        
        if (this.position == newPosition) {
            //nothing to do
            return this;
        }
        else if (this.position < newPosition && newPosition - this.position < SKIP_DISTANCE) {
         retryHandler.tryOnceThenWithRetries(() -> {
                     // if the current position is before new position but nearby do not open a new connection
                     // but skip the bytes until the new position
                     long bytesToSkip = newPosition - this.position;
                     backingStream.skipNBytes(bytesToSkip);
                     LOGGER.debug("Skipped {} bytes out of {} when setting position to {} (previously on {})",
                             bytesToSkip, bytesToSkip, newPosition, position);
                     return null;
                 },
                 () -> {
                     closeSilently();
                     openChannel(newPosition);
                     return null;
                 });
        } else {
            // in this case, we require to re-instantiate the channel
            // opening at the new position - and closing the previous
            closeSilently();
            retryHandler.runWithRetries(() -> openChannel(newPosition));
        }
        // update to the new position
        this.position = newPosition;
        return this;
    }

    @Override
    public synchronized long size() throws IOException {
        assertChannelIsOpen();
        retryHandler.runWithRetries( () -> {
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
                    throw new InterruptedIOException("Interrupted while trying to get size of file at " + uri.toString());
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
    private synchronized void openChannel(final long position) throws IOException {
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
            throw new IOException("Failed to connect to " + uri + " at position: " + position, ex);
        } catch (final InterruptedException ex) {
            throw new InterruptedIOException("Interrupted while connecting to " + uri + " at position: " + position);
        }
        assertGoodHttpResponse(response, isRangeRequest);
        backingStream = new BufferedInputStream(response.body());
        channel = Channels.newChannel(backingStream);
        this.position = position;
    }
}
