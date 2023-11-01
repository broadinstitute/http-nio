package org.broadinstitute.http.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
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
public class NewURLSeekableByteChannel implements SeekableByteChannel {


    private static final long SKIP_DISTANCE = 8 * 1024;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    // url and proxy for the file
    private final URI uri;

    // current position of the SeekableByteChannel
    private long position = 0;

    // the size of the whole file (-1 is not initialized)
    private long size = -1;

    private HttpClient client = null;
    private ReadableByteChannel channel = null;
    private InputStream backingStream = null;


    NewURLSeekableByteChannel(String string) throws IOException, URISyntaxException {
        this(new URI(string));
    }

    NewURLSeekableByteChannel(URL url) throws URISyntaxException, IOException {
        this(url.toURI());
    }

    NewURLSeekableByteChannel(final URI uri) throws IOException {
        this.uri = Utils.nonNull(uri, () -> "null URI");
        this.client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        // and instantiate the stream/channel at position 0
        instantiateChannel(0L);

    }

    @Override
    public synchronized int read(final ByteBuffer dst) throws IOException {
        final int read = channel.read(dst);
        if( read != -1) {
            this.position += read;
        }
        return read;
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
    public synchronized NewURLSeekableByteChannel position(long newPosition) throws IOException {
        if (newPosition < 0) {
            throw new IllegalArgumentException("Cannot seek a negative position");
        }
        if (!isOpen()) {
            throw new ClosedChannelException();
        }
        if (this.position == newPosition){
            //nothing to do here
        } else if (this.position < newPosition && newPosition - this.position < SKIP_DISTANCE) {
            // if the current position is before but nearby do not open a new connection
            // but skip the bytes until the new position
            long bytesToSkip = newPosition - this.position;
            while(bytesToSkip > 0) {
                final long skipped = backingStream.skip(bytesToSkip);
                if( skipped <= 0){
                    throw new IOException("Failed to skip any bytes while moving from " + this.position + " to " + newPosition);
                }
                bytesToSkip -= skipped;
            }
            logger.debug("Skipped {} bytes out of {} for setting position to {} (previously on {})",
                    bytesToSkip, bytesToSkip, newPosition, position);
        } else  {
            // in this case, we require to re-instantiate the channel
            // opening at the new position - and closing the previous
            close();
            instantiateChannel(newPosition);
        }

        // updates to the new position
        this.position = newPosition;

        return this;
    }

    @Override
    public synchronized long size() throws IOException {
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
                checkResponse(response, false);
                final Map<String, List<String>> map = response.headers().map();
                final List<String> contentLengthStrings = map.get("content-length");
                if( contentLengthStrings == null || contentLengthStrings.size() != 1){
                    throw new IOException("Failed to get size of file at " + uri.toString() + "," +
                            " content-length=" + contentLengthStrings);
                } else {
                    size = Long.parseLong(contentLengthStrings.get(0));
                }
            } catch (InterruptedException e) {
                throw new IOException("Interrupted while trying to get size of file at " + uri.toString() , e);
            }
        }
        return size;
    }

    private void checkResponse(final HttpResponse<?> response, boolean isRangeRequest) throws IOException {
       int code = response.statusCode();
       switch (code) {
               case 200 -> {
                   if (isRangeRequest) {
                       throw new IOException("Server returned entire file instead of subrange for " + uri);
                   }
               }
               case 206 -> {
                   if (!isRangeRequest) {
                       throw new IOException("Unexpected Partial Content result for request for entire file at " + uri);
                   }
               }
               case 404 -> throw new FileNotFoundException("File not found at " + uri);
               default -> throw new IOException("Unexpected http response code: " + code + " when requesting " + uri);
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

    // open a readable byte channel for the requested position
    private synchronized void instantiateChannel(final long position) throws IOException{
            final HttpRequest.Builder builder = HttpRequest.newBuilder(uri).GET();
            final boolean isRangeRequest = position != 0;
            if(isRangeRequest){
                builder.setHeader("Range", "bytes="+position+"-");
            }
            HttpRequest request = builder.build();

        final HttpResponse<InputStream> response;
        try {
           response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        } catch (final FileNotFoundException ex) {
            throw ex;
        } catch (final IOException ex){
            throw new IOException("Failed to connect to " + uri + "at positon: " + position, ex);
        } catch (final InterruptedException ex){
            throw new IOException("Interrupted while connecting to " + uri + " at position: " + position, ex);
        }
        checkResponse(response, isRangeRequest);
        backingStream = new BufferedInputStream(response.body());
        channel = Channels.newChannel(backingStream);
        this.position = position;
    }

}
