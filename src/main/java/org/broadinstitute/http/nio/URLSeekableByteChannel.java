package org.broadinstitute.http.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

/**
 * Implementation for a {@link SeekableByteChannel} for {@link URL} open as a connection.
 *
 * <p>The current implementation is thread-safe using the {@code synchronized} keyword in every
 * method.
 *
 * @author Daniel Gomez-Sanchez (magicDGS)
 * @implNote this seekable byte channel is read-only.
 */
class URLSeekableByteChannel implements SeekableByteChannel {

    private static final long SKIP_DISTANCE = 8 * 1024;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    // url and proxy for the file
    private final URL url;

    // current position of the SeekableByteChannel
    private long position = 0;

    // the size of the whole file (-1 is not initialized)
    private long size = -1;

    private URLConnection connection = null;
    private ReadableByteChannel channel = null;
    private InputStream backingStream = null;


    URLSeekableByteChannel(final URL url) throws IOException {
        this.url = Utils.nonNull(url, () -> "null URL");
        // and instantiate the stream/channel at position 0
        instantiateChannel(this.position);
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
    public synchronized URLSeekableByteChannel position(long newPosition) throws IOException {
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
            final URLConnection connection = url.openConnection();
            connection.connect();
            // try block for always disconnect the connection
            try {
                size = connection.getContentLengthLong();
                // if the size is still -1, it means that it is unavailable
                if (size == -1) {
                    throw new IOException("Unable to retrieve content length for " + url);
                }
            } finally {
                // disconnect if possible
               if( connection != null) {
                   HttpUtils.disconnect(connection);
               }
            }
        }
        return size;
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

        if( connection != null ) {
            HttpUtils.disconnect(connection);
            connection = null;
        }
    }

    // open a readable byte channel for the requested position
    private synchronized void instantiateChannel(final long position) throws IOException {
        try {
            if( connection == null) {
                connection = url.openConnection();
            }

            if (position > 0) {
                HttpUtils.setRangeRequest(connection, position, -1);
            }

            //TODO BufferedInputStream might be unecessary
            backingStream = new BufferedInputStream(connection.getInputStream());
            channel = Channels.newChannel(backingStream);
            this.position = position;
        } catch (final FileNotFoundException ex){
            throw ex;
        } catch (final IOException ex){
            throw new IOException("Failure while instantiating connection to: " + url.toString() + " at position: " + position, ex);
        }
    }



}
