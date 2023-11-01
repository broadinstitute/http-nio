package org.broadinstitute.http.nio;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.SeekableByteChannel;

/**
 * @author Daniel Gomez-Sanchez (magicDGS)
 */
public class URLSeekableByteChannelUnitTest extends HttpSeekableByteChannelUnitTest {

    @Override
    protected SeekableByteChannel getChannel(final URL url) throws IOException, URISyntaxException {
        return new URLSeekableByteChannel(url);
    }


}
