package org.broadinstitute.http.nio;

import org.broadinstitute.http.nio.utils.HttpUtils;
import org.broadinstitute.http.nio.utils.Utils;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract {@link FileSystemProvider} for {@link HttpFileSystem}.
 *
 * <p>HTTP/S are handled in the same way in jsr203-http, but every protocol requires its own
 * provider to return its scheme with {@link #getScheme()}.
 *
 * @author Daniel Gomez-Sanchez (magicDGS)
 */
abstract class HttpAbstractFileSystemProvider extends FileSystemProvider {
    private static HttpFileSystemProviderSettings settings = HttpFileSystemProviderSettings.DEFAULT_SETTINGS;

    // map of authorities and FileSystem - using a concurrent implementation for thread-safety
    private final Map<String, HttpFileSystem> fileSystems = new ConcurrentHashMap<>();

    /**
     * {@inheritDoc}
     *
     * @implNote should return a valid http/s scheme.
     */
    @Override
    public abstract String getScheme();

    // check the conditions for an URI, and return it if it is correct
    private URI checkUri(final URI uri) {
        // non-null URI
        Utils.nonNull(uri, () -> "null URI");
        // non-null authority
        Utils.nonNull(uri.getAuthority(),
                () -> String.format("%s requires URI with authority: invalid %s", this, uri));
        // check the scheme (sanity check)
        if (!getScheme().equalsIgnoreCase(uri.getScheme())) {
            throw new ProviderMismatchException(String.format("Invalid scheme for %s: %s",
                    this, uri.getScheme()));
        }
        return uri;
    }

    @Override
    public final HttpFileSystem newFileSystem(final URI uri, final Map<String, ?> env) {
        checkUri(uri);

        if (fileSystems.containsKey(uri.getAuthority())) {
            throw new FileSystemAlreadyExistsException("URI: " + uri);
        }

        return fileSystems.computeIfAbsent(uri.getAuthority(),
                (auth) -> new HttpFileSystem(this, auth));
    }

    @Override
    public final HttpFileSystem getFileSystem(final URI uri) {
        final HttpFileSystem fs = fileSystems.get(checkUri(uri).getAuthority());
        if (fs == null) {
            throw new FileSystemNotFoundException("URI: " + uri);
        }
        return fs;
    }

    @Override
    public final HttpPath getPath(final URI uri) {
        checkUri(uri);
        return fileSystems
                .computeIfAbsent(uri.getAuthority(), (auth) -> new HttpFileSystem(this, auth))
                .getPath(uri);
    }

    @Override
    public final SeekableByteChannel newByteChannel(final Path path,
            final Set<? extends OpenOption> options, final FileAttribute<?>... attrs)
            throws IOException {
        Utils.nonNull(path, () -> "null path");
        Utils.nonNull(options, () -> "null options");
        // the URI is only checked after asserting if the conditions are met, otherwise it will throw
        // an unsupported operation exception
        if (options.isEmpty() ||
                (options.size() == 1 && options.contains(StandardOpenOption.READ))) {
            // convert Path to URI and check it to see if there is a mismatch with the provider
            final URI uri = path.toUri();
            checkUri(uri);

            // return an HttpSeekableByteChannel
            return new HttpSeekableByteChannel(uri, settings, 0L);
        }
        throw new UnsupportedOperationException(
                String.format("Only %s is supported for %s, but %s options(s) are provided",
                        StandardOpenOption.READ, this, options));
    }


    @Override
    public final DirectoryStream<Path> newDirectoryStream(final Path dir,
            final DirectoryStream.Filter<? super Path> filter) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** Unsupported method. */
    @Override
    public final void createDirectory(final Path dir, final FileAttribute<?>... attrs)
            throws IOException {
        throw new UnsupportedOperationException(this.getClass().getName() +
                " is read-only: cannot create directory");
    }

    /** Unsupported method. */
    @Override
    public final void delete(final Path path) throws IOException {
        throw new UnsupportedOperationException(this.getClass().getName() +
                " is read-only: cannot delete directory");
    }

    @Override
    public final void copy(final Path source, final Path target, CopyOption... options)
            throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** Unsupported method. */
    @Override
    public final void move(final Path source, final Path target, final CopyOption... options)
            throws IOException {
        throw new UnsupportedOperationException(this.getClass().getName() +
                " is read-only: cannot move paths");
    }

    @Override
    public final boolean isSameFile(final Path path, final Path path2) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public final boolean isHidden(final Path path) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public final FileStore getFileStore(final Path path) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public final void checkAccess(final Path path, final AccessMode... modes) throws IOException {
        Utils.nonNull(path, () -> "null path");
        // get the URI (use also for exception messages)
        final URI uri = checkUri(path.toUri());
        if (!HttpUtils.exists(uri, settings)){
            throw new NoSuchFileException(uri.toString());
        }
        for (AccessMode access : modes) {
            if (Objects.requireNonNull(access) != AccessMode.READ) {
                throw new UnsupportedOperationException("Unsupported access mode: " + access);
            }
        }
    }

    @Override
    public final <V extends FileAttributeView> V getFileAttributeView(final Path path,
            final Class<V> type, final LinkOption... options) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <A extends BasicFileAttributes> A readAttributes(final Path path,
            final Class<A> type, final LinkOption... options) throws IOException {
        if ( type.equals(HttpBasicFileAttributes.class) || type.equals(BasicFileAttributes.class)) {
            return  (A) new HttpBasicFileAttributes();
        } else {
            throw new UnsupportedOperationException("Can't provide attributes of the given type: " + type.getCanonicalName());
        }
    }

    @Override
    public final Map<String, Object> readAttributes(final Path path, final String attributes,
            final LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public final void setAttribute(final Path path, final String attribute, final Object value,
            final LinkOption... options) throws IOException {
        throw new UnsupportedOperationException(this.getClass().getName() +
                " is read-only: cannot set attributes to paths");
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    /** @return the current settings */
    public static synchronized HttpFileSystemProviderSettings getSettings(){
        return settings;
    }

    /** override the existing settings
     * @param settings the new settings object to use*/
    public static synchronized void setSettings(HttpFileSystemProviderSettings settings){
        HttpAbstractFileSystemProvider.settings = settings;
    }

    private static class HttpBasicFileAttributes implements BasicFileAttributes {

        @Override
        public FileTime lastModifiedTime() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public FileTime lastAccessTime() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public FileTime creationTime() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public boolean isRegularFile() {
            return true;
        }

        @Override
        public boolean isDirectory() {
            return false;
        }

        @Override
        public boolean isSymbolicLink() {
            return false;
        }

        @Override
        public boolean isOther() {
            return false;
        }

        @Override
        public long size() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public Object fileKey() {
            throw new UnsupportedOperationException("Not implemented");
        }
    }
}
