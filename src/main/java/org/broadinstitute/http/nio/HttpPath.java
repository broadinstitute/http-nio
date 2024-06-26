package org.broadinstitute.http.nio;

import org.broadinstitute.http.nio.utils.HttpUtils;
import org.broadinstitute.http.nio.utils.Utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * {@link Path} for HTTP/S.
 *
 * <p>The HTTP/S paths holds the following information:
 *
 * <ul>
 *
 * <li>
 * The {@link HttpFileSystem} originating the path. The protocol is retrieved, if necessary,
 * from the provider of the File System.
 * </li>
 *
 * <li>
 * The hostname and domain for the URL/URI in a single authority String.
 * </li>
 *
 * <li>
 * If present, the path component of the URL/URI.
 * </li>
 *
 * <li>
 * If present, the query and reference Strings.
 * </li>
 *
 * </ul>
 *
 * @author Daniel Gomez-Sanchez (magicDGS)
 */
final class HttpPath implements Path {

    // file system (indicates the scheme - HTTP or HTTPS)
    private final HttpFileSystem fs;

    // path - similar to other implementation of Path
    //Stored in encoded form
    private final byte[] normalizedPath;

    // offsets for the separator (computed if needed)
    private volatile int[] offsets;

    // query for the URL (may be null)
    //Stored in encoded form
    private final String query;

    // reference for the URL (may be null) / fragment for the URI representation
    //Stored in encoded form
    private final String reference;

    // true if the path is absolute; false otherwise
    private final boolean absolute;

    /**
     * Internal constructor.
     *
     * @param fs             file system. Shouldn't be {@code null}.
     * @param query          query. May be {@code null}.
     * @param reference      reference. May be {@code null}.
     * @param normalizedPath normalized path (as a byte array). Shouldn't be {@code null}.
     * @implNote does not perform any check for efficiency.
     */
    private HttpPath(final HttpFileSystem fs,
            final String query,
            final String reference,
            final boolean absolute,
            final byte... normalizedPath) {
        this.fs = fs;

        // optional query and reference components (may be null)
        this.query = query;
        this.reference = reference;

        // set the absolute status
        this.absolute = absolute;

        // normalized path bytes (shouldn't be null)
        this.normalizedPath = Utils.nonNull(normalizedPath,  () -> "path may not be null");
    }

    /**
     * Creates a new Path in the provided {@link HttpFileSystem}, with optional query and reference.
     *
     * @param fs        file system representing the base URL (scheme and authority).
     * @param path      path (absolute) component for the URL (required).
     * @param query     query  component for the URL (optional).
     * @param reference reference component for the URL (optional).
     */
    HttpPath(final HttpFileSystem fs, final String path, final String query,
             final String reference) {
        // always absolute and checking it when converting to byte[]
        this(Utils.nonNull(fs, () -> "null fs"), query, reference, true,
                getNormalizedPathBytes(Utils.nonNull(path, () -> "null path"), true));
    }

    @Override
    public HttpFileSystem getFileSystem() {
        return fs;
    }

    @Override
    public boolean isAbsolute() {
        return absolute;
    }

    @Override
    public Path getRoot() {
        // root is a Path with only the byte array (always absolute)
        return new HttpPath(fs, null, null, true, new byte[0]);
    }

    /**
     * {@inheritDoc}
     *
     * @implNote returns always a relative path.
     */
    @Override
    public Path getFileName() {
        initOffsets();
        // following the contract, for the getNameCounts() == 0 (root) we return null
        if (offsets.length == 0) {
            return null;
        }
        // file names are always relative paths
        return subpath(offsets.length - 1, offsets.length, false);
    }

    /**
     * {@inheritDoc}
     *
     * @implNote returned path keeps the {@link #isAbsolute()} status of the current path.
     */
    @Override
    public Path getParent() {
        initOffsets();
        // returns the root if there is no
        if (offsets.length == 0) {
            return getRoot();
        }
        // parent names are absolute/relative depending on the current status
        return subpath(0, offsets.length - 1, absolute);
    }

    @Override
    public int getNameCount() {
        initOffsets();
        return offsets.length;
    }

    /**
     * {@inheritDoc}
     *
     * @implNote returns always a relative path.
     */
    @Override
    public Path getName(final int index) {
        initOffsets();
        // returns always a relative path
        return subpath(index, index + 1, false);
    }

    @Override
    public Path subpath(final int beginIndex, final int endIndex) {
        initOffsets();
        // following the contract for invalid indexes
        if (beginIndex < 0 || beginIndex >= offsets.length ||
                endIndex <= beginIndex || endIndex > offsets.length) {
            throw new IllegalArgumentException(String
                    .format("Invalid indexes for path with %s name(s): [%s, %s]",
                            getNameCount(), beginIndex, endIndex));
        }
        // return the new path (always relative path following the contract)
        return subpath(beginIndex, endIndex, false);
    }

    /**
     * Helper method to implement different subpath routines with different absolute/relative
     * status.
     *
     * <p>The contract of this method is the same as {@link Path#subpath(int, int)}).
     *
     * @param beginIndex the index of the first element, inclusive
     * @param endIndex   the index of the last element, exclusive
     * @param absolute   {@code true} if the returned path is absolute; {@code false} otherwise.
     *
     * @return a new path object that is a subsequence of the nams elements in this {@code
     * HttpPath}.
     *
     * @implNote assumes that the caller already initialized the offsets and that the indexes are
     * correct.
     */
    private HttpPath subpath(final int beginIndex, final int endIndex, final boolean absolute) {
        // get the coordinates to copy the path array
        final int begin = offsets[beginIndex];
        final int end = (endIndex == offsets.length) ? normalizedPath.length : offsets[endIndex];

        // construct the result
        final byte[] newPath = Arrays.copyOfRange(normalizedPath, begin, end);

        // return the new path (always relative path)
        // TODO: should the query/reference be propagated?
        return new HttpPath(this.fs, null, null, absolute, newPath);
    }

    @Override
    public boolean startsWith(final Path other) {
        // different FileSystems return false
        if (!this.getFileSystem().equals(Utils.nonNull(other, () -> "null path").getFileSystem())) {
            return false;
        }

        return startsWith(((HttpPath) other).normalizedPath);
    }

    @Override
    public boolean startsWith(final String other) {
        // throw if null
        Utils.nonNull(other, () -> "null other");
        // normalize the path and check with the byte method
        return startsWith(getNormalizedPathBytes(other, false));
    }

    /**
     * Private method to test startsWith only for the path component.
     *
     * <p>The contract for this method is the same as {@link #startsWith(Path)} (Path)}, but only
     * for the path component.
     *
     * @param other the other path component.
     *
     * @return {@code true} if {@link #normalizedPath} ends with {@code other}; {@code false}
     * otherwise.
     */
    private boolean startsWith(final byte[] other) {
        // the other can still end in '/', so we should trim
        final int olen = getLastIndexWithoutTrailingSlash(other);

        // the other path component cannot have a larger than this for startWith
        if (olen > normalizedPath.length) {
            return false;
        }

        // check the bytes of the normalized path
        int i;
        for (i = 0; i <= olen; i++) {
            if (normalizedPath[i] != other[i]) {
                return false;
            }
        }

        // finally check the name boundary
        return i >= this.normalizedPath.length
                || this.normalizedPath[i] == HttpUtils.HTTP_PATH_SEPARATOR_CHAR;
    }

    @Override
    public boolean endsWith(final Path other) {
        // different FileSystems return false
        if (!this.getFileSystem().equals(Utils.nonNull(other, () -> "null path").getFileSystem())) {
            return false;
        }

        // compare the path component
        // TODO: maybe we should use isAbsolute() after https://github.com/magicDGS/jsr203-http/issues/12
        return endsWith(((HttpPath) other).normalizedPath, true);
    }

    @Override
    public boolean endsWith(final String other) {
        // throw if null
        Utils.nonNull(other, () -> "null other");
        // normalize the path and check with the byte method
        return endsWith(getNormalizedPathBytes(other, false), false);
    }

    /**
     * Private method to test endsWith only for the path component.
     *
     * <p>The contract for this method is the same as {@link #endsWith(Path)}, but only for the
     * path component.
     *
     * @param other the other path component.
     * @param pathVersion if {@code false}, perform an extra check for the String version.
     * @return {@code true} if {@link #normalizedPath} ends with {@code other}; {@code false}
     * otherwise.
     */
    private boolean endsWith(final byte[] other, final boolean pathVersion) {
        // get the last index to check
        int olast = getLastIndexWithoutTrailingSlash(other);
        // get the last index to check
        int last = getLastIndexWithoutTrailingSlash(this.normalizedPath);

        // early termination if the length is 0 (last index = -1)
        if (olast == -1) {
            return last == -1;
        }
        // early termination if the other is larger
        if (last < olast) {
            return false;
        }

        // iterate over the bytes to check if they are the same
        for (; olast >= 0; olast--, last--) {
            if (other[olast] != this.normalizedPath[last]) {
                return false;
            }
        }

        // last == -1 when olast == -1 (the same length and equals)
        if (last == -1) {
            return true;
        }

        // switch for the path version or not path version
        if (pathVersion) {
            // at this point, the pathVersion should always return true (as it is bounded)
            // TODO: this might change after relative path support (https://github.com/magicDGS/jsr203-http/issues/12)
            return true;
        } else {
            // otherwise, it shouldn't be included (e.g., "/foo/bar" ends with "bar" but not "/bar"
            return this.normalizedPath[last] == HttpUtils.HTTP_PATH_SEPARATOR_CHAR;
        }
    }

    @Override
    public Path normalize() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * {@inheritDoc}
     *
     * @implNote This implementation differs from the expected behavior of the method when other is absolute. Instead
     * of returning other in that case it will throw {@link UnsupportedOperationException}.
     */
    @Override
    public HttpPath resolve(final Path other) {
        if(other == null){
            return this;
        }  else if(other.isAbsolute()){
            //Note: This violates the general contract of the method but shouldn't be important practically.
                throw new UnsupportedOperationException("Cannot resolve an absolute path against an http(s) path."
                        + "\nThis path is: " + this
                        + "\nThe problematic path is an instance of " + other.getClass().getName()
                        + "\nOther path: " + other);
        } else {
            try {
                final URI otherUri = new URI(other.toString()); // to string is used here instead of the expected toUri because
                                                                // toUri will produce normalized absolute paths in many filesystems which is
                                                                // what we don't want
                return resolve(otherUri);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Can only resolve http(s) paths against fully encoded paths which are valid URIs.", e);
            }
        }
    }

    private HttpPath resolve(URI other){
        return new HttpPath(fs, other.getRawQuery(),
                other.getRawFragment(),
                this.isAbsolute(),
                concatPaths(this.normalizedPath, getNormalizedPathBytes(other.getRawPath(), false)));
    }

    @Override
    public HttpPath resolve(final String other) {
        return resolve(fromRelativeString(other));  // Paths.get() and the filesystem equivalent can't be used here
                                                    // because we don't allow them to create relative HttpPaths 
    }

    @Override
    public Path resolveSibling(final String other){
        return resolveSibling(fromRelativeString(other));
    }

    @Override
    public Path relativize(final Path other) {
        throw new UnsupportedOperationException("Not implemented");
    }

    private HttpPath fromRelativeString(final String other) {
        if (other == null) {
            return null;
        } else {
            try {
                final URI uri = new URI(other);
                if (uri.isAbsolute()) {
                    throw new UnsupportedOperationException("Resolving absolute URI strings against an HTTP path is not supported." +
                            "\nURI: " + uri);
                }
                return new HttpPath(getFileSystem(), uri.getRawFragment(), uri.getRawQuery(), false,
                        getNormalizedPathBytes(uri.getRawPath(), false));
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Cannot resolve against an invalid URI.", e);
            }
        }
    }

    @Override
    public URI toUri() {
        try {
            return new URI(toUriString(true));
        } catch (final URISyntaxException e) {
            throw new IOError(e);
        }
    }

    @Override
    public Path toAbsolutePath() {
        if (isAbsolute()) {
            return this;
        }
        // just fromUri a new path with a different absolute status
        return new HttpPath(fs, query, reference, true,normalizedPath);
    }

    @Override
    public Path toRealPath(final LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** Unsupported method. */
    @Override
    public File toFile() {
        throw new UnsupportedOperationException(this.getClass() + " cannot be converted to a File");
    }

    @Override
    public WatchKey register(final WatchService watcher, final WatchEvent.Kind<?>[] events,
            final WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public WatchKey register(final WatchService watcher, final WatchEvent.Kind<?>... events)
            throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Iterator<Path> iterator() {
        return IntStream.range(0, getNameCount()).mapToObj(this::getName).iterator();
    }

    /**
     * {@inheritDoc}
     *
     * @implNote comparison of every component of the HTTP/S path is case-sensitive, except the
     * scheme and the authority.
     * @implNote if the query and/or reference are present, this method order the one without any
     * of them first.
     */
    @Override
    public int compareTo(final Path other) {
        if (this == other) {
            return 0;
        }

        final HttpPath httpOther = (HttpPath) other;
        // object comparison - should be from the same provider
        if (fs.provider() != httpOther.fs.provider()) {
            throw new ClassCastException();
        }

        // first check the authority (case insensitive)
        int comparison = fs.getAuthority().compareToIgnoreCase(httpOther.fs.getAuthority());
        if (comparison != 0) {
            return comparison;
        }

        // then check the path
        final int len1 = normalizedPath.length;
        final int len2 = httpOther.normalizedPath.length;
        final int n = Math.min(len1, len2);
        for (int k = 0; k < n; k++) {
            // this is case sensitive
            comparison = Byte.compare(this.normalizedPath[k], httpOther.normalizedPath[k]);
            if (comparison != 0) {
                return comparison;
            }
        }
        comparison = len1 - len2;
        if (comparison != 0) {
            return comparison;
        }

        // compare the query if present
        comparison = Comparator.nullsFirst(String::compareTo).compare(this.query, httpOther.query);
        if (comparison != 0) {
            return comparison;
        }

        // otherwise, just return the value of comparing the fragment
        return Comparator.nullsFirst(String::compareTo)
                .compare(this.reference, httpOther.reference);
    }

    /**
     * {@inheritDoc}
     *
     * @implNote it uses the {@link #compareTo(Path)} method and the absolute status.
     */
    @Override
    public boolean equals(final Object other) {
        try {
            return ((HttpPath) other).absolute == this.absolute && compareTo((Path) other) == 0;
        } catch (ClassCastException e) {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @implNote Includes the absolute status and  all the components of the path in a
     * case-sensitive way, except the scheme and the authority.
     */
    @Override
    public int hashCode() {
        // TODO - maybe we should cache (https://github.com/magicDGS/jsr203-http/issues/18)
        int h = 31 * Boolean.hashCode(absolute) + fs.hashCode();
        for (int i = 0; i < normalizedPath.length; i++) {
            h = 31 * h + (normalizedPath[i] & 0xff);
        }
        // this is safe for null query and reference
        h = 31 * h + Objects.hash(query, reference);
        return h;
    }

    @Override
    public String toString() {
        return toUriString(isAbsolute());
    }

    private String toUriString(boolean includeRoot) {
        // TODO - maybe we should cache (https://github.com/magicDGS/jsr203-http/issues/18)
        // adding scheme, authority and normalized path
        final StringBuilder sb = new StringBuilder();
        if(includeRoot) {
            sb.append(fs.provider().getScheme()) // scheme
                    .append("://")
                    .append(fs.getAuthority()) // authority
                    .append(new String(normalizedPath, HttpUtils.HTTP_PATH_CHARSET));
        }  else if( normalizedPath.length != 0){
            if(normalizedPath[0] == HttpUtils.HTTP_PATH_SEPARATOR_CHAR) {
                sb.append(new String(normalizedPath, 1, normalizedPath.length - 1, HttpUtils.HTTP_PATH_CHARSET));
            } else {
                 sb.append(new String(normalizedPath, HttpUtils.HTTP_PATH_CHARSET));
            }
        }
        if (query != null) {
            sb.append('?').append(query);
        }
        if (reference != null) {
            sb.append('#').append(reference);
        }
        return sb.toString();
    }

    /**
     * Creates the array of offsets if not already created.
     *
     * @implNote it assumes that redundant separators are already removed.
     */
    private void initOffsets() {
        if (offsets == null) {
            // get the length without the trailing slash
            final int length = getLastIndexWithoutTrailingSlash(normalizedPath);
            // count names
            int count = 0;
            // index position (outside loop to re-use in the next loop)
            int index = 0;
            for (; index < length; index++) {
                final byte c = normalizedPath[index];
                if (c == HttpUtils.HTTP_PATH_SEPARATOR_CHAR) {
                    count++;
                    // assumes that redundant separators are already removed
                    index++;
                }
            }
            // populate offsets
            final int[] result = new int[count];
            count = 0;
            for (index = 0; index < length; index++) {
                final byte c = normalizedPath[index];
                if (c == HttpUtils.HTTP_PATH_SEPARATOR_CHAR) {
                    // assumes that redundant separators are already removed
                    result[count++] = index++;
                }
            }
            // update in a thread-safe manner
            synchronized (this) {
                if (offsets == null) {
                    offsets = result;
                }
            }
        }
    }

    /**
     * Gets the path as a normalized (without multiple slashes) array of bytes.
     *
     * @param path          path to convert into byte[]
     * @param checkRelative if {@code true}, check if the path is absolute.
     *
     * @return array of bytes, without multiple slashes together.
     */
    private static byte[] getNormalizedPathBytes(final String path, final boolean checkRelative) {
        if (checkRelative && !path.isEmpty() && !path.startsWith(HttpUtils.HTTP_PATH_SEPARATOR_STRING)) {
            throw new InvalidPathException(path, "Relative HTTP/S path are not supported");
        }

        if (HttpUtils.HTTP_PATH_SEPARATOR_STRING.equals(path) || path.isEmpty()) {
            return new byte[0];
        }
        final int len = path.length();

        char prevChar = 0;
        for (int i = 0; i < len; i++) {
            char c = path.charAt(i);
            if (isDoubleSeparator(prevChar, c)) {
                return getNormalizedPathBytes(path, len, i - 1);
            }
            prevChar = checkNotNull(path, c);
        }
        if (prevChar == HttpUtils.HTTP_PATH_SEPARATOR_CHAR) {
            return getNormalizedPathBytes(path, len, len - 1);
        }

        return path.getBytes(HttpUtils.HTTP_PATH_CHARSET);
    }

    private static byte[] getNormalizedPathBytes(final String path, final int len,
            final int offset) {
        // get first the last offset
        int lastOffset = len;
        while (lastOffset > 0
                && path.charAt(lastOffset - 1) == HttpUtils.HTTP_PATH_SEPARATOR_CHAR) {
            lastOffset--;
        }
        if (lastOffset == 0) {
            // early termination
            return new byte[] {HttpUtils.HTTP_PATH_SEPARATOR_CHAR};
        }
        // byte output stream
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream(len)) {
            if (offset > 0) {
                os.write(path.substring(0, offset).getBytes(HttpUtils.HTTP_PATH_CHARSET));
            }
            char prevChar = 0;
            for (int i = offset; i < len; i++) {
                char c = path.charAt(i);
                if (isDoubleSeparator(prevChar, c)) {
                    continue;
                }
                prevChar = checkNotNull(path, c);
                os.write(c);
            }

            return os.toByteArray();
        } catch (final IOException e) {
            throw new Utils.ShouldNotHappenException(e);
        }
    }

    private static boolean isDoubleSeparator(final char prevChar, final char c) {
        return c == HttpUtils.HTTP_PATH_SEPARATOR_CHAR
                && prevChar == HttpUtils.HTTP_PATH_SEPARATOR_CHAR;
    }

    private static char checkNotNull(final String path, char c) {
        if (c == '\u0000') {
            throw new InvalidPathException(path, "Null character not allowed in path");
        }
        return c;
    }

    /**
     * Gets the last index to consider in the path bytes.
     *
     * <p>If the lst index is a trailing slash {@link HttpUtils#HTTP_PATH_SEPARATOR_CHAR}, it
     * should not be considered for some operations. This method takes into account that problem.
     *
     * @param path bytes representing the path.
     *
     * @return last index of path to consider.
     */
    private static int getLastIndexWithoutTrailingSlash(final byte[] path) {
        int len = path.length - 1;
        if (len > 0 && path[len] == HttpUtils.HTTP_PATH_SEPARATOR_CHAR) {
            len--;
        }
        return len;
    }

    private static byte[] concatPaths(byte[] array1, byte[] array2) {
        int array1ModifiedLength = getLastIndexWithoutTrailingSlash(array1) + 1;
        byte[] result = Arrays.copyOf(array1, array1ModifiedLength + 1 + array2.length );
        result[array1ModifiedLength] = HttpUtils.HTTP_PATH_SEPARATOR_CHAR;
        System.arraycopy(array2, 0, result, array1ModifiedLength + 1, array2.length);
        return result;
    }
    
}
