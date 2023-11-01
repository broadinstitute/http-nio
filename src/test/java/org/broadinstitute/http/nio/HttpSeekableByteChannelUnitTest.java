package org.broadinstitute.http.nio;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

@Test
public abstract class HttpSeekableByteChannelUnitTest extends BaseTest {
    public static final String LARGE_FILE_ON_GOOGLE = "https://storage.googleapis.com/hellbender/test/resources/benchmark/CEUTrio.HiSeq.WEx.b37.NA12892.bam";
    public static final String BIG_TXT_GOOGLE = "https://storage.googleapis.com/hellbender/test/resources/nio/big.txt";
    public static final String BIG_TXT_LOCAL = "testdata/big.txt";

    @Test(expectedExceptions = FileNotFoundException.class)
    public void testNonExistentUrl() throws Exception {
        getChannel(getGithubPagesFileUrl("not_existent.txt"));
    }

    @Test
    public void testNonWritableChannelExceptions() throws Exception {
        try (final SeekableByteChannel channel = getChannel(getGithubPagesFileUrl("file1.txt"))) {
            // cannot write
            Assert.assertThrows(NonWritableChannelException.class,
                    () -> channel.write(ByteBuffer.allocate(10)));
            // cannot truncate
            Assert.assertThrows(NonWritableChannelException.class, () -> channel.truncate(10));
        }
    }

    @Test(dataProvider = "getDocsFilesForTesting", dataProviderClass = GitHubResourcesIntegrationTest.class)
    public void testSize(final String fileName) throws Exception {
        final URL urlFile = getGithubPagesFileUrl(fileName);
        final Path localFile = getLocalDocsFilePath(fileName);
        try (final SeekableByteChannel urlChannel = getChannel(urlFile);
             final SeekableByteChannel localChannel = Files.newByteChannel(localFile)) {
            Assert.assertEquals(urlChannel.size(), localChannel.size());
        }
    }

    @Test(dataProvider = "getDocsFilesForTesting", dataProviderClass = GitHubResourcesIntegrationTest.class)
    public void testSizeAfterRead(final String fileName) throws Exception {
        final URL urlFile = getGithubPagesFileUrl(fileName);
        final Path localFile = getLocalDocsFilePath(fileName);
        try (final SeekableByteChannel urlChannel = getChannel(urlFile);
             final SeekableByteChannel localChannel = Files.newByteChannel(localFile)) {

            // use the local channel size to read
            testReadSize((int) localChannel.size(),
                    urlChannel,
                    localChannel);

            // check that the size after read is the same
            Assert.assertEquals(urlChannel.size(), localChannel.size());
        }
    }

    @Test(dataProvider = "getDocsFilesForTesting", dataProviderClass = GitHubResourcesIntegrationTest.class)
    public void testCachedSize(final String fileName) throws Exception {
        final URL urlFile = getGithubPagesFileUrl(fileName);
        final Path localFile = getLocalDocsFilePath(fileName);
        try (final SeekableByteChannel urlChannel = getChannel(urlFile);
             final SeekableByteChannel localChannel = Files.newByteChannel(localFile)) {

            // test that the size before reading is the same (is cached)
            Assert.assertEquals(urlChannel.size(), localChannel.size());

            // use the local channel size to read
            testReadSize((int) localChannel.size(),
                    urlChannel,
                    localChannel);

            // check that the cached size in the URL sbc is the same as the local
            Assert.assertEquals(urlChannel.size(), localChannel.size());
        }
    }

    @Test
    public void testGetPosition() throws Exception {
        // open channel
        try (final SeekableByteChannel channel = getChannel(getGithubPagesFileUrl("file1.txt"))) {
            int currentPosition = 0;
            Assert.assertEquals(channel.position(), currentPosition);
            final int bufferSize = Math.round(channel.size() / 10f);
            final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            for (int i = bufferSize; i <= channel.size(); i += bufferSize) {
                channel.read(buffer);
                Assert.assertEquals(channel.position(), i);
                buffer.rewind();
            }
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testIllegalPosition() throws Exception {
        getChannel(getGithubPagesFileUrl("file1.txt")).position(-1);
    }

    protected abstract SeekableByteChannel getChannel(URL url) throws IOException, URISyntaxException;

    protected static void testReadSize(final int size,
                                       final SeekableByteChannel actual, final SeekableByteChannel expected)
            throws Exception {
        final ByteBuffer expectedBuffer = ByteBuffer.allocate(size);
        final ByteBuffer actualBuffer = ByteBuffer.allocate(size);
        Assert.assertEquals(
                actual.read(expectedBuffer),
                expected.read(actualBuffer),
                "different number of bytes read");
        expectedBuffer.rewind();
        actualBuffer.rewind();
        Assert.assertEquals(expectedBuffer.array(), actualBuffer.array(),
                "different byte[] after read");
    }

    public static byte[] readAll(int length, ReadableByteChannel channel) throws IOException {
        final ByteBuffer buff = ByteBuffer.allocate(length);
        int read = 0;
        while (read < length) {
            final int lastRead = channel.read(buff);
            if (lastRead == -1) {
                break;
            }
            read += lastRead;
        }
        return buff.array();
    }

    @DataProvider
    public Iterator<Object[]> seekData() {
        return Stream.of(GitHubResourcesIntegrationTest.getDocsFilesForTesting())
                .map(data -> (String) data[0]).map(fileName ->
                        new Object[]{
                                getGithubPagesFileUrl(fileName), // URL
                                10,                              // position to seek
                                getLocalDocsFilePath(fileName)   // local file
                        }).iterator();
    }

    @Test(dataProvider = "seekData")
    public void testSeekBeforeRead(final URL testUrl, final long position, final Path localFile)
            throws Exception {
        try (final SeekableByteChannel actual = getChannel(testUrl);
             final SeekableByteChannel expected = Files.newByteChannel(localFile)) {
            HttpSeekableByteChannelUnitTest.testReadSize((int) expected.size(),
                    actual.position(position),
                    expected.position(position));
        }
    }

    @Test(dataProvider = "seekData")
    public void testSeekToBeginning(final URL testUrl, final long position, final Path localFile)
            throws Exception {
        try (final SeekableByteChannel actual = getChannel(testUrl);
             final SeekableByteChannel expected = Files.newByteChannel(localFile)) {
            HttpSeekableByteChannelUnitTest.testReadSize((int) expected.size(),
                    // first position and then come back to 0
                    actual.position(position).position(0),
                    expected);
        }
    }

    @Test(dataProvider = "seekData")
    public void testSeekToSamePosition(final URL testUrl, final long position, final Path localFile)
            throws Exception {
        try (final SeekableByteChannel actual = getChannel(testUrl);
             final SeekableByteChannel expected = Files.newByteChannel(localFile)) {
            HttpSeekableByteChannelUnitTest.testReadSize((int) expected.size(),
                    // seek twice to the same position is equal to seek only once
                    actual.position(position).position(position),
                    expected.position(position));
        }
    }

    @Test(dataProvider = "seekData")
    public void testSeekShouldReopen(final URL testUrl, final long position, final Path localFile)
            throws Exception {
        try (final SeekableByteChannel actual = getChannel(testUrl);
             final SeekableByteChannel expected = Files.newByteChannel(localFile)) {
            HttpSeekableByteChannelUnitTest.testReadSize((int) expected.size(),
                    // seek first to 10 bytes more, and then to the requested position
                    actual.position(position + 10).position(position),
                    expected.position(position));
        }
    }

    @Test
    public void testClose() throws Exception {
        // open channel
        final SeekableByteChannel channel = getChannel(getGithubPagesFileUrl("file1.txt"));
        Assert.assertTrue(channel.isOpen());
        // close channel
        channel.close();
        Assert.assertFalse(channel.isOpen());

        // assert that several methods thrown with the corresponding exception
        // 1. position
        Assert.assertThrows(ClosedChannelException.class, () -> channel.position());
        Assert.assertThrows(ClosedChannelException.class, () -> channel.position(10));
        // 2. size
        Assert.assertThrows(ClosedChannelException.class, () -> channel.size());
        // 3. read
        Assert.assertThrows(ClosedChannelException.class, () -> channel.read(ByteBuffer.allocate(1)));
    }

    @Test
    public void testPositionOnGoogleFile() throws IOException, URISyntaxException {
        URL url = new URL(LARGE_FILE_ON_GOOGLE);
        try (SeekableByteChannel channel = getChannel(url)) {
            channel.position(10000);
            channel.read(ByteBuffer.allocate(100));
        }
    }

    @Test(timeOut = 10_000L)
    //time out if it's taking over 10 seconds to read, that indicates we're probably not skipping correctly
    public void testReadAtEndOfLargeFileIsFast() throws IOException, URISyntaxException {
        URL url = new URL(LARGE_FILE_ON_GOOGLE);
        try (SeekableByteChannel channel = getChannel(url)) {
            long size = channel.size();
            Assert.assertEquals(size, 31710132189L);
            channel.position(size - 200);
            Assert.assertTrue(channel.read(ByteBuffer.allocate(100)) > 0);
        }
    }

    @DataProvider
    public Object[][] stepSize() {
        return new Object[][]{{100}, {1_000}, {10_000}, {100_000}, {1_000_000}};
    }

    @Test(dataProvider = "stepSize")
    public void testDataIsConsistentForward(int stepSize) throws IOException, URISyntaxException {
        URL url = new URL(BIG_TXT_GOOGLE);
        try (final SeekableByteChannel remote = getChannel(url);
             final SeekableByteChannel local = Files.newByteChannel(Paths.get(BIG_TXT_LOCAL))) {
            while (remote.position() < remote.size()) {
                System.out.println("pos: " + remote.position() + " size: " + remote.size());
                Assert.assertEquals(HttpSeekableByteChannelUnitTest.readAll(stepSize, remote), HttpSeekableByteChannelUnitTest.readAll(stepSize, local));
            }
        }
    }

    @DataProvider
    public Object[][] steps() {
        return new Object[][]{
                {Arrays.asList(1, 100, 1000, 10000, 100_000)},
                {Arrays.asList(100_000, 10_000, 1_000, 100, 1, 0)},
                {Arrays.asList(1000, 999, 1000, 999, 1000, 999)},
                {Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)},
                {Arrays.asList(100, 151, 10_000, 824823, 10_000, 151, 100)}
        };
    }

    @Test(dataProvider = "steps")
    public void testDataIsConsistentWhileSeeking(List<Integer> positions) throws IOException, URISyntaxException {
        URL url = new URL(BIG_TXT_GOOGLE);
        final int readlength = 100;
        try (final SeekableByteChannel remote = getChannel(url);
             final SeekableByteChannel local = Files.newByteChannel(Paths.get(BIG_TXT_LOCAL))) {
            for (int position : positions) {
                remote.position(position);
                local.position(position);
                System.out.println("Start pos: " + remote.position() + " size: " + remote.size());
                Assert.assertEquals(HttpSeekableByteChannelUnitTest.readAll(readlength, remote), HttpSeekableByteChannelUnitTest.readAll(readlength, local));
                System.out.println("End pos: " + remote.position() + " size: " + remote.size());
            }
        }
    }
}
