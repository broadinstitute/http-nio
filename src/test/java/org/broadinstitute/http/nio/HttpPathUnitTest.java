package org.broadinstitute.http.nio;

import htsjdk.samtools.SamFiles;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.StreamSupport;



/**
 * @author Daniel Gomez-Sanchez (magicDGS)
 */
public class HttpPathUnitTest extends BaseTest {

    private static final HttpFileSystemProvider TEST_FS_PROVIDER = new HttpFileSystemProvider();
    private static final String TEST_AUTHORITY = "example.com";
    private final HttpFileSystem TEST_FS = new HttpFileSystem(TEST_FS_PROVIDER, TEST_AUTHORITY);

    private static final HttpPath createPathFromUriOnTestProvider(final URI uri) {
        return new HttpPath(
                new HttpFileSystem(TEST_FS_PROVIDER, uri.getRawAuthority()),
                uri.getRawPath(), uri.getRawQuery(), uri.getRawFragment());
    }

    private static final HttpPath createPathFromUriStringOnTestProvider(final String uriString) {
        return createPathFromUriOnTestProvider(URI.create(uriString));
    }

    @DataProvider
    public Object[][] invalidConstructorArgs() {
        return new Object[][] {
                {"relative_path", TEST_FS, IllegalArgumentException.class},
                {"null_\0_in_path", TEST_FS, InvalidPathException.class}
        };
    }

    @Test(dataProvider = "invalidConstructorArgs")
    public void testInvalidConstruction(final String path, final HttpFileSystem fs, Class<Throwable> exception) {
        Assert.assertThrows(exception, () -> new HttpPath(fs, path, null, null));
    }

    @DataProvider
    public Object[][] authoritiesToTest() {
        return new Object[][] {
                {TEST_AUTHORITY},
                {"example.org"},
                {"hello.world.net"},
                {"user:password@example.com"},
                {"example.org:100"},
                {"user:password@example.org:100"}
        };
    }

    @Test(dataProvider = "authoritiesToTest")
    public void testGetRoot(final String authority) {
        final HttpFileSystem fs = new HttpFileSystem(TEST_FS_PROVIDER, authority);
        final HttpPath testPath = new HttpPath(fs, "/example.html", null, null);
        // should only be one root, this might fail if the FileSystem returns more
        for (final Path root : fs.getRootDirectories()) {
            assertEqualsPath(root, testPath.getRoot());
        }
    }

    @DataProvider
    public Object[][] startsWithData() {
        final String dir = "/dir";
        final String file = "/file.html";
        return new Object[][] {
                // force codepaht for longer other
                {TEST_FS.getPath(file), dir + file, false},
                // exactly the same for construction
                {TEST_FS.getPath(dir), dir, true},
                {TEST_FS.getPath(file), file, true},
                // only directory
                {TEST_FS.getPath(dir + file), dir, true},
                // both directory and file
                {TEST_FS.getPath(dir + file), dir + file, true},
                // several directories
                {TEST_FS.getPath(dir + dir + dir), dir + dir, true},
                // truncated start
                {TEST_FS.getPath(file), "/" + file.substring(2), false},
                // truncated end
                {TEST_FS.getPath(file), file.substring(0, file.length()-1), false},
                {TEST_FS.getPath(dir + file), dir + file.substring(0, file.length()-1), false},
                // tess for directories and trailing /
                // Path directory without trailing, and other with
                {TEST_FS.getPath(dir), dir + "/", true},
                // Path directory with trailing /, and other without
                {TEST_FS.getPath(dir + "/"), dir, true},
                // edge-case: root directory
                {TEST_FS.getPath("/"), "/", true}
        };
    }

    @Test(dataProvider = "startsWithData")
    public void testStartsWith(final Path path, final String other, final boolean expected) {
        Assert.assertEquals(path.startsWith(other), expected);
    }


    @Test(dataProvider = "startsWithData")
    public void testStartsWithPath(final Path path, final String other, final boolean expected) {
        final Path otherPath = TEST_FS.getPath(other);
        Assert.assertEquals(path.startsWith(otherPath), expected);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testStartsWithNullPath() {
        TEST_FS.getPath("/file.txt").startsWith((Path) null);
    }

    @Test
    public void testStartsWithRelativePath() {
        final Path path = TEST_FS.getPath("/dir/dir/dir");
        // check endsWith a relative subpath
        Assert.assertTrue(path.startsWith(path.subpath(2,3)));
    }

    @Test
    public void testStartsWithDifferentProvider() {
        final Path httpPath = TEST_FS.getPath("/file.txt");
        final Path localPath = new File("/file.txt").toPath();
        Assert.assertFalse(httpPath.startsWith(localPath));
    }

    @DataProvider
    public Object[][] endsWithData() {
        final String dir = "/dir";
        final String file = "/file.html";
        return new Object[][] {
                // force codepaht for longer other
                {TEST_FS.getPath(file), dir + file, false},
                // exactly the same for construction
                {TEST_FS.getPath(dir), dir, true},
                {TEST_FS.getPath(file), file, true},
                {TEST_FS.getPath(dir + file), dir + file, true},
                // only file
                {TEST_FS.getPath(dir + file), file, true},
                // several directories
                {TEST_FS.getPath(dir + dir + file), dir + file, true},
                // truncated start
                {TEST_FS.getPath(file), "/" + file.substring(2), false},
                {TEST_FS.getPath(dir + file), dir + "/" + file.substring(2), false},
                // not bounded
                {TEST_FS.getPath(file), file.replaceFirst("/", "/a"), false},
                {TEST_FS.getPath(dir + file), file.replaceFirst("/", "/a"), false},
                // truncated start
                {TEST_FS.getPath(file), "/" + file.substring(2), false},
                // truncated start
                {TEST_FS.getPath(file), file.substring(0, file.length()-1), false},
                {TEST_FS.getPath(dir + file), dir + file.substring(0, file.length()-1), false},
                // tess for directories and trailing /
                // Path directory without trailing, and other with
                {TEST_FS.getPath(dir), dir + "/", true},
                // Path directory with trailing /, and other without
                {TEST_FS.getPath(dir + "/"), dir, true},
                // edge-case: root directory
                {TEST_FS.getPath("/"), "", true},
                {TEST_FS.getPath("/"), "/", true},
                {TEST_FS.getPath(file), "/", false}
        };
    }

    @Test(dataProvider = "endsWithData")
    public void testEndsWith(final Path path, final String otherWithRootComponent, final boolean expected) {
        Assert.assertEquals(path.endsWith(otherWithRootComponent.replaceFirst("/", "")), expected);
    }

    @Test
    public void testEndsWithStringAbsolutePath() {
        final Path path = TEST_FS.getPath("/foo/bar");
        // following the contract, "http://example.com/foo/bar".endsWith("/bar") should return false
        Assert.assertFalse(path.endsWith("/bar"));
        // but if I understood correctly, if it endsWith("/foo/bar") should return true
        Assert.assertTrue(path.endsWith("/foo/bar"));
    }

    @Test(dataProvider = "endsWithData")
    public void testEndsWithPath(final Path path, final String other, final boolean expected) {
        final Path otherPath = TEST_FS.getPath(other);
        Assert.assertEquals(path.endsWith(otherPath), expected);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEndsWithNullPath() {
        TEST_FS.getPath("/file.txt").endsWith((Path) null);
    }

    @Test
    public void testEndsWithRelativePath() {
        final Path path = TEST_FS.getPath("/first/second/third");
        // check endsWith the subpath
        Assert.assertTrue(path.endsWith(path.subpath(1, 3)));
        Assert.assertTrue(path.endsWith(path.getFileName()));
    }

    @Test
    public void testEndsWithDifferentProvider() {
        final Path httpPath = TEST_FS.getPath("/file.txt");
        final Path localPath = new File("/file.txt").toPath();
        Assert.assertFalse(httpPath.endsWith(localPath));
    }

    @DataProvider
    public Object[][] fileNames() {
        return new Object[][] {
                {"/index.html", "index.html"},
                {"/dir/index.html", "index.html"},
                {"/dir1/dir2/index.html", "index.html"},
                // should also work with redundant paths (as we are already normalizing)
                {"/dir//index.html", "index.html"},
                {"/dir1//dir2//index.txt", "index.txt"},
                //Check we ignore queries and fragments
                {"/dir1//dir2//index.txt?query=hello#2", "index.txt"},
                //check encoding
                {"/dir1//dir%202//index.txt", "index.txt"},
                {"/dir1//dir2//index%20file.txt", "index%20file.txt"}

        };
    }

    @Test(dataProvider = "fileNames")
    public void testGetFileName(final String pathWithoutAuthority, final String expectedName) {
        final HttpPath path = TEST_FS.getPath(pathWithoutAuthority);
        // the best way to check if it is correct is to check the string representation
        // because the equal method should include the absolute status
        Assert.assertEquals(path.getFileName().toString(),
                expectedName);
        // file names are never absolute
        Assert.assertFalse(path.getFileName().isAbsolute());
    }

    @Test
    public void testGetFileNameForRoot() {
        Assert.assertNull(TEST_FS.getPath("/").getFileName());
    }

    @DataProvider
    public static Object[][] parentData() {
        return new Object[][] {
                {"/dir/index.html", "/dir"},
                {"/dir1/dir2/index.html", "/dir1/dir2"},
                // should also work with redundant paths (as we are already normalizing)
                {"/dir//index.html", "/dir"},
                {"/dir1//dir2//index.txt", "/dir1/dir2"},
                //Check we ignore queries and fragments
                {"/dir1//dir2//index.txt?query=hello#2", "/dir1/dir2"},
                //check encoding
                {"/dir1//dir%202//index.txt", "/dir1/dir%202"},
                {"/dir1//dir2//index%20file.txt", "/dir1/dir2"}
        };
    }

    @Test(dataProvider = "parentData")
    public void testGetParentAbsolute(final String pathWithoutAuthority,
            final String expectedParent) {
        final HttpPath path = TEST_FS.getPath(pathWithoutAuthority);
        final Path absoluteParent = path.getParent();
        // check that the paths are the same (even absolute in this case)
        assertEqualsPath(absoluteParent, TEST_FS.getPath(expectedParent));
        Assert.assertTrue(absoluteParent.isAbsolute());
    }

    @Test(dataProvider = "parentData")
    public void testGetParentRelative(final String pathWithoutAuthority,
            final String expectedParent) {
        final HttpPath path = TEST_FS.getPath(pathWithoutAuthority);
        // get first a relative path by using subpath, and then the parent of it
        final Path relativeParent = path.subpath(0, path.getNameCount() - 1).getParent();
        // the best way to check if it is correct is to check the string representation
        // because the equal method should include the absolute status
        Assert.assertEquals(path.getParent().toString(),
                TEST_FS.getPath(expectedParent).toString());
        Assert.assertFalse(relativeParent.isAbsolute());
    }

    @Test
    public void testGetParentIsRoot() {
        // gets a file sited on the root path
        final HttpPath path = TEST_FS.getPath("/index.html");
        // check that the parent and the root are the same
        assertEqualsPath(path.getParent(), path.getRoot());
        // and that as root, it is always absolute
        Assert.assertTrue(path.getParent().isAbsolute());
    }

    @Test
    public void testGetParentForRoot() {
        final HttpPath root = TEST_FS.getPath("/");
        assertEqualsPath(root.getParent(), root);
    }

    @DataProvider
    public Object[][] nameCounts() {
        return new Object[][] {
                // contract says that root returns 0 counts
                {"http://" + TEST_AUTHORITY, 0},
                {"http://" + TEST_AUTHORITY + "/", 0},
                // files (never trailing slash)
                {"http://" + TEST_AUTHORITY + "/index.html", 1},
                {"http://" + TEST_AUTHORITY + "/dir1/index.html", 2},
                {"http://" + TEST_AUTHORITY + "/dir1/dir2/index.html", 3},
                // directories (with and without trailing slash)
                {"https://" + TEST_AUTHORITY + "/dir", 1},
                {"https://" + TEST_AUTHORITY + "/dir/", 1},
                {"https://" + TEST_AUTHORITY + "/dir1/dir2", 2},
                {"https://" + TEST_AUTHORITY + "/dir1/dir2/", 2},

        };
    }

    @Test(dataProvider = "nameCounts")
    public void testGetNameCount(final String uriString, final int count)
            throws MalformedURLException {
        final HttpPath path = createPathFromUriStringOnTestProvider(uriString);
        Assert.assertEquals(path.getNameCount(), count);
        Assert.assertEquals(StreamSupport.stream(path.spliterator(), false).count(), count);
        // check that getName(i) does not fail
        for (int i = 0; i < path.getNameCount(); i++) {
            Assert.assertNotNull(path.getName(i));
        }
    }

    @DataProvider
    public Object[][] invalidIndexSubpath() {
        final HttpPath testPath = TEST_FS.getPath("/dir1/dir2/dir3/index.html");
        return new Object[][]{
                // for the root one, it should not work
                {testPath.getRoot(), 0, 1},
                // negative start and end
                {testPath, -1, 1},
                {testPath, 1, -1},
                // lower end than start
                {testPath, 2, 1},
                // larger end than counts
                {testPath, 1, testPath.getNameCount() + 1},
                // larger start than counts
                {testPath, testPath.getNameCount() + 1, 2}
        };
    }

    @Test(dataProvider = "invalidIndexSubpath", expectedExceptions = IllegalArgumentException.class)
    public void testInvalidIndexSubpath(final HttpPath path, final int beginIndex, final int endIndex) {
        path.subpath(beginIndex, endIndex);
    }

    @DataProvider
    public Object[][] validUriStrings() {
        return new Object[][] {
                {"http://example.com"},
                {"http://example.com/index.html"},
                {"http://example.com/file.txt?query=hello+world"},
                {"http://example.com/file.pdf#1"},
                {"http://example.com/file.txt?query=hello+world#2"},
                {"http://example.com/directory/file.gz"},
                {"http://example.com/directory/file.gz?query=hello+world"},
                {"http://example.com/directory/file.pdf#1"},
                {"http://example.com/file.gz?query=hello+world#2"},
                {"http://example.com/file.gz?query=hello%20world#2"},
                {"http://example.com/file%201.gz"},
                {"http://example.com/file%201.gz?query=hello%20world#2%203"},
        };
    }

    @Test(dataProvider = "validUriStrings")
    public void testToUri(final String uriString) throws MalformedURLException {
        final URI uri = URI.create(uriString);
        final HttpPath path = createPathFromUriOnTestProvider(uri);
        Assert.assertNotSame(path.toUri(), uri);
        Assert.assertEquals(path.toUri(), uri);
        Assert.assertEquals(path.toUri().toURL(), uri.toURL());
    }

    @Test
    public void testRelativeToUri() {
        final String fileString = "/file.html";
        final Path path = TEST_FS.getPath("/dir1/dir2", fileString).getFileName();
        final URI expected = URI.create("http://" + TEST_AUTHORITY + fileString);
        Assert.assertEquals(path.toUri(), expected);
    }

    @Test
    public void testToAbsolutePath() {
        final HttpPath path = TEST_FS.getPath("/dir/index.html");
        Assert.assertSame(path.toAbsolutePath(), path);
    }

    @Test
    public void testToAbsolutePathFromRelative() {
        final HttpPath path = TEST_FS.getPath("/dir/index.html");
        final HttpPath expectedAbsolute = TEST_FS.getPath("/index.html");
        assertEqualsPath(path.getFileName().toAbsolutePath(), expectedAbsolute);
    }

    @Test
    public void testIterator() {
        final String[] parts = new String[] {"/dir1", "/dir2", "/index.html"};
        final HttpPath path = TEST_FS.getPath(String.join(""));
        int index = 0;
        for (final Path next : path) {
            // check as a String (because it is relative)
            Assert.assertEquals(next.toString(), TEST_FS.getPath(parts[index]).toString(),
                    "index=" + index);
            // check that it is relative
            Assert.assertFalse(next.isAbsolute(), "index=" + index);
            // check the Path::getName
            Assert.assertEquals(next, path.getName(index), "index=" + index);
        }
    }

    @DataProvider
    public Object[][] compareToUriStrings() {
        // default values for testing
        final String auth1 = "example.com";
        final String file1 = "file1.txt";
        final String query1 = "query=true";
        final String ref1 = "1";
        // against the following
        final String auth2 = "example.org";
        final String file2 = "file2.txt";
        final String query2 = "query=false";
        final String ref2 = "2";


        return new Object[][] {
                // completely equal addresses (incrementing components
                {
                        "http://" + auth1,
                        "http://" + auth1,
                        0
                },
                {
                        "http://" + auth1 + "/" + file1,
                        "http://" + auth1 + "/" + file1,
                        0
                },
                {
                        "http://" + auth1 + "/" + file1 + "?" + query1,
                        "http://" + auth1 + "/" + file1 + "?" + query1,
                        0
                },
                {
                        "http://" + auth1 + "/" + file1 + "?" + query1 + "#" + ref1,
                        "http://" + auth1 + "/" + file1 + "?" + query1 + "#" + ref1,
                        0
                },
                // case-insensitive authority
                {
                        "http://" + auth1.toLowerCase(),
                        "http://" + auth1.toUpperCase(),
                        0
                },
                // authority order
                {
                        "http://" + auth1,
                        "http://" + auth2,
                        auth1.compareTo(auth2)
                },
                // authority order independent of file name
                {
                        "http://" + auth1 + "/" + file1,
                        "http://" + auth2 + "/" + file2,
                        auth1.compareTo(auth2)
                },
                {
                        "http://" + auth1 + "/" + file2,
                        "http://" + auth2 + "/" + file1,
                        auth1.compareTo(auth2)
                },
                // file order for same authority
                {
                        "http://" + auth1 + "/" + file1,
                        "http://" + auth1 + "/" + file2,
                        file1.compareTo(file2)
                },
                // including different lengths (e.g., compressed)
                {
                        "http://" + auth1 + "/" + file1,
                        "http://" + auth1 + "/" + file1 + ".gz",
                        // difference in length (".gz" = 3 chars)
                        -3
                },
                // and it is independent of the query
                {
                        "http://" + auth1 + "/" + file1 + "?" + query1,
                        "http://" + auth1 + "/" + file2 + "?" + query2,
                        file1.compareTo(file2)
                },
                {
                        "http://" + auth1 + "/" + file1 + "?" + query2,
                        "http://" + auth1 + "/" + file2 + "?" + query1,
                        file1.compareTo(file2)
                },
                // query order if case of equal authority and file
                {
                        "http://" + auth1 + "/" + file1 + "?" + query1,
                        "http://" + auth1 + "/" + file1 + "?" + query2,
                        query1.compareTo(query2)
                },
                // and it is independent of the ref
                {
                        "http://" + auth1 + "/" + file1 + "?" + query1 + "#" + ref1,
                        "http://" + auth1 + "/" + file1 + "?" + query2 + "#" + ref2,
                        query1.compareTo(query2)
                },
                {
                        "http://" + auth1 + "/" + file1 + "?" + query1 + "#" + ref2,
                        "http://" + auth1 + "/" + file1 + "?" + query2 + "#" + ref1,
                        query1.compareTo(query2)
                },
                // only different references
                {
                        "http://" + auth1 + "/" + file1 + "?" + query1 + "#" + ref1,
                        "http://" + auth1 + "/" + file1 + "?" + query1 + "#" + ref2,
                        ref1.compareTo(ref2)
                }
        };
    }

    @Test(dataProvider = "compareToUriStrings")
    public void testCompareTo(final String uriString1, final String uriString2, final int result) {
        final HttpPath firstPath = createPathFromUriStringOnTestProvider(uriString1);
        final HttpPath secondPath = createPathFromUriStringOnTestProvider(uriString2);
        Assert.assertEquals(firstPath.compareTo(secondPath), result);
    }

    @Test
    public void testCompareToDifferentProviders() {
        final String path = "/index.html";
        final HttpPath httpPath = new HttpPath(TEST_FS, path, null, null);
        final HttpPath httpsPath = new HttpPath(
                new HttpFileSystem(new HttpsFileSystemProvider(), TEST_AUTHORITY),
                path, null, null);
        Assert.assertThrows(ClassCastException.class, () -> httpPath.compareTo(httpsPath));
    }

    @Test(dataProvider = "compareToUriStrings")
    public void testEquals(final String first, final String second, final int result) {
        final HttpPath firstPath = createPathFromUriStringOnTestProvider(first);
        final HttpPath secondPath = createPathFromUriStringOnTestProvider(second);
        if (result == 0) {
            assertEqualsPath(firstPath, secondPath);
        } else {
            assertNotEqualsPath(firstPath, secondPath);
        }
    }

    @Test
    public void testEqualsDifferentObject() {
        final String uriString = "http://example.com";
        final HttpPath path = createPathFromUriStringOnTestProvider(uriString);
        Assert.assertFalse(path.equals(uriString));
    }

    @Test
    public void testEqualsSamObject() {
        final HttpPath path = createPathFromUriStringOnTestProvider("http://example.com/index.html");
        assertEqualsPath(path, path);
    }

    @Test
    public void testEqualsDifferentProvider() {
        final HttpPath httpPath = createPathFromUriStringOnTestProvider("http://" + TEST_AUTHORITY);
        final HttpPath httpsPath = new HttpPath(
                new HttpFileSystem(new HttpsFileSystemProvider(), TEST_AUTHORITY),
                "", null, null);
        assertNotEqualsPath(httpPath, httpsPath);
    }

    @Test
    public void testEqualsAbsoluteRelative() {
        final HttpPath absolute = TEST_FS.getPath("/index.html");
        final Path relative = absolute.subpath(0, absolute.getNameCount());
        // sanity check in case something change in the implementation
        Assert.assertFalse(relative.isAbsolute(), "error in test data");
        assertNotEqualsPath(absolute, relative);
    }

    @Test(dataProvider = "validUriStrings")
    public void testHashCodeSameObject(final String uriString) {
        final HttpPath path = createPathFromUriStringOnTestProvider(uriString);
        Assert.assertEquals(path.hashCode(), path.hashCode());
    }

    @Test(dataProvider = "validUriStrings")
    public void testHashCodeEqualObjects(final String uriString) {
        Assert.assertEquals(createPathFromUriStringOnTestProvider(uriString).hashCode(),
                createPathFromUriStringOnTestProvider(uriString).hashCode());
    }

    @Test
    public void testHashCodeAbsoluteRelativeDiffers() {
        final HttpPath absolute = TEST_FS.getPath("/index.html");
        final Path relative = absolute.subpath(0, absolute.getNameCount());
        // sanity check in case something change in the implementation
        Assert.assertFalse(relative.isAbsolute(), "error in test data");
        Assert.assertNotEquals(absolute.hashCode(), relative.hashCode());
    }

    @Test(dataProvider = "validUriStrings")
    public void testToString(final String uriString) {
        final HttpPath path = createPathFromUriStringOnTestProvider(uriString);
        Assert.assertEquals(path.toString(), uriString);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testToFile() {
        new HttpPath(TEST_FS, "", null, null).toFile();
    }

    @DataProvider
    public Object[][] severalSlashesPaths() {
        return new Object[][] {
                {"//dir//file.txt", "/dir/file.txt"},
                {"/dir//file.txt", "/dir/file.txt"},
                {"//dir/file.txt", "/dir/file.txt"}
        };
    }

    @Test(dataProvider = "severalSlashesPaths")
    public void testNormalizeSeveralSlashes(final String withSlashes, final String withoutSlashes) {
        // this test that, independently of the number of slashes, the stored Path is normalized
        assertEqualsPath(
                new HttpPath(TEST_FS, withSlashes, null, null),
                new HttpPath(TEST_FS, withoutSlashes, null, null));
    }

    @DataProvider
    public Object[][] toResolve(){
        return new Object[][]{
            {getHttpPath("http://hello.com"), null, getHttpPath("http://hello.com")},
        //    {getHttpPath("http://hello.com"), getHttpPath("http://goodbye.com"), getHttpPath("http://goodbye.com")},  resolving against absolute paths is unsupported
            {getHttpPath("http://hello.com"), Paths.get("file.txt"), getHttpPath("http://hello.com/file.txt")},
            {getHttpPath("http://hello.com/"), Paths.get("file.txt"), getHttpPath("http://hello.com/file.txt")},
            {getHttpPath("https://hello.com"), Paths.get("file.txt"), getHttpPath("https://hello.com/file.txt")},
            {getHttpPath("https://hello.com/subdir"),
                    Paths.get("deeper/file.txt"),
                    getHttpPath("https://hello.com/subdir/deeper/file.txt")},

            {getHttpPath("https://hello.com/subdir/"),
                    Paths.get("deeper/file.txt"),
                    getHttpPath("https://hello.com/subdir/deeper/file.txt")},

            {getHttpPath("https://hello.com/subdir/file.txt?something=somethingelse,boom=shakalaka#hereIam"),
                    Paths.get("subfolder?this=mine#thereItis"),
                    getHttpPath("https://hello.com/subdir/file.txt/subfolder?this=mine#thereItis")},

            {getHttpPath("https://hello.com/subdir/file%20path?something=somethingelse,boom=shakalaka#hereIam"),
                    Paths.get("sub%20folder?this=mi%20ne#thereIt%20is"),
                    getHttpPath("https://hello.com/subdir/file%20path/sub%20folder?this=mi%20ne#thereIt%20is")}
        };
    }
    @Test(dataProvider = "toResolve")
    public void testResolve(HttpPath first, Path second, HttpPath expected){
        final Path actual = first.resolve(second);
        Assert.assertEquals(actual, expected, "\nexpected:\t " + expected + "\nactual:\t\t" + actual);
    }



    @DataProvider
    public Object[][] resolveStrings(){
        return new Object[][]{
                {getHttpPath("http://hello.com"), null, getHttpPath("http://hello.com")}, // if null return this
               // {getHttpPath("http://hello.com"), "file.txt", getHttpPath("http://hello.com/file.txt")}, //if other is absolute return other <-  this is unsupported
                {getHttpPath("http://hello.com"), "", getHttpPath("http://hello.com")},
                {getHttpPath("http://hello.com/"), "file.txt", getHttpPath("http://hello.com/file.txt")},
                {getHttpPath("https://hello.com"), "file.txt", getHttpPath("https://hello.com/file.txt")},
                {getHttpPath("https://hello.com/subdir"),
                        "deeper/file.txt",
                        getHttpPath("https://hello.com/subdir/deeper/file.txt")},

                {getHttpPath("https://hello.com/subdir/"),
                        "deeper/file.txt",
                        getHttpPath("https://hello.com/subdir/deeper/file.txt")},

                {getHttpPath("https://hello.com/subdir/file.txt?something=somethingelse,boom=shakalaka#hereIam"),
                        "subfolder?this=mine#thereItis",
                        getHttpPath("https://hello.com/subdir/file.txt/subfolder?this=mine#thereItis")},

                {getHttpPath("https://hello.com/subdir/file%20path?something=somethingelse,boom=shakalaka#hereIam"),
                        "sub%20folder?this=mi%20ne#thereIt%20is",
                        getHttpPath("https://hello.com/subdir/file%20path/sub%20folder?this=mi%20ne#thereIt%20is")}
        };
    }

    @Test(dataProvider = "resolveStrings")
    public void testResolveString(HttpPath path, String resolveString, HttpPath expected){
        Assert.assertEquals(path.resolve(resolveString), expected);
    }

    @DataProvider
    public Object[][] absoluteStringsToResolveAgainst() {
            return new Object[][]{
                    {"http://example.com"},
                    {"http://example.com/file.txt"},
                    {"https://example.com"},
                    {"https://example.com/"},
                    {"https://example.com/file.txt"},
                    {"file:///local"},
                    {"c://local"},
                    {"file:///local/file"}
            };
    }

    @Test(dataProvider = "absoluteStringsToResolveAgainst", expectedExceptions = UnsupportedOperationException.class)
    public void testResolveAgainstAbsoluteFailsStrings(String other){
        getHttpPath("http://www.example.com/file.txt").resolve(other);
    }

    @Test(dataProvider = "absoluteStringsToResolveAgainst", expectedExceptions = UnsupportedOperationException.class)
    public void testResolveAgainstAbsoluteFailsStringsDomainOnly(String other){
        getHttpPath("http://www.example.com").resolve(other);
    }

    @DataProvider
    public Object[][] getSiblingTests() {
        return new Object[][]{
                {getHttpPath("http://hello.com/"), "", "http://hello.com"},
                {getHttpPath("http://hello.com"), "file.txt", "http://hello.com/file.txt"},
                {getHttpPath("http://hello.com/"), "file.txt", "http://hello.com/file.txt"},
                {getHttpPath("http://hello.com/file.tx"), "picture.jpg", "http://hello.com/picture.jpg"},

                {getHttpPath("http://hello.com/subdir/subdir2/"), "picture.jpg", "http://hello.com/subdir/picture.jpg"},
                {getHttpPath("http://hello.com/subdir/subdir2"), "picture.jpg", "http://hello.com/subdir/picture.jpg"},

                {getHttpPath("http://hello.com/subdir/subdir2/?query=yes#hashtag"), "picture.jpg?query=maybe#octothorpe", "http://hello.com/subdir/picture.jpg?query=maybe#octothorpe"},
                {getHttpPath("http://hello.com/subdir/subdir2?query=yes#hashtag"), "picture.jpg?query=maybe#", "http://hello.com/subdir/picture.jpg?query=maybe#"},
        };
    }

    @Test(dataProvider = "getSiblingTests")
    public void testResolveSiblingString(HttpPath path, String toResolve, String expected){
        Assert.assertEquals(path.resolveSibling(toResolve), getHttpPath(expected));
    }

    @Test(dataProvider = "getSiblingTests")
    public void testResolveSiblingPath(HttpPath path, String toResolve, String expected){
        Path other = Paths.get(toResolve);
        Assert.assertEquals(path.resolveSibling(other), getHttpPath(expected));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullSiblingIsNPE(){
        getHttpPath("http://hello.com").resolveSibling((String)null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUnencoded() {
        getHttpPath("https://hello.com/subdir/file%20path?something=somethingelse,boom=shakalaka#hereIam").resolve(Paths.get("sub folder?this=mi ne#thereIt is"));
    }

    public static HttpPath getHttpPath(String path) {
        return (HttpPath)Paths.get(URI.create(path));
    }

    @DataProvider
    public static Object[][] relativePaths() {
        final HttpPath absolutePath = getHttpPath("https://example.com/pile/of/fastas/fasta.gz");
        final HttpPath absolutePathWithQueryAndReference = getHttpPath("https://example.com/pile/of/fastas/fasta.gz?hi=there#kablam");
        return new Object[][]{
                {absolutePath.subpath(0, 4), "pile/of/fastas/fasta.gz"},
                {absolutePath.subpath(1, 3), "of/fastas"},
                {absolutePath.subpath(3, 4), "fasta.gz"},
                {absolutePathWithQueryAndReference.subpath(0, 4), "pile/of/fastas/fasta.gz"}, //query and reference are not kept by subpath
                {absolutePathWithQueryAndReference.subpath(1, 3), "of/fastas"},
                {absolutePathWithQueryAndReference.subpath(3, 4), "fasta.gz"},
        };
    }

    @Test(dataProvider = "relativePaths")
    public void testRelativePathsToString(Path subpath, String expected){
        Assert.assertEquals(subpath.toString(), expected);
    }

    // fix for https://github.com/broadinstitute/gatk/issues/8751
    @Test
    public void testResolveFasta() {
        Path fastaFile = getHttpPath("https://example.com/fastas/fasta.gz");
        final Path fileNamePath = fastaFile.getFileName();
        final String indexName = fileNamePath + ".fai";
        Assert.assertEquals(fastaFile.resolveSibling(indexName).toString(), "https://example.com/fastas/fasta.gz.fai");
    }

    @Test
    // similar bug to test resolve fasta, but calling into the htsjdk method itself
    public void testWithHtsjdkFindIndex(){
        final Path index = SamFiles.findIndex(getHttpPath("http://example.com/example.bam"));
        Assert.assertEquals(index, getHttpPath("http://example.com/example.bai"));
    }
}
