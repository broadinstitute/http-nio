package org.broadinstitute.http.nio;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.SeekableByteChannel;

public class NewURLSeekableByteChannelUnitTest extends HttpSeekableByteChannelUnitTest{

    @Override
    protected SeekableByteChannel getChannel(URL url) throws URISyntaxException, IOException {
        return new NewURLSeekableByteChannel(url);
    }
}
