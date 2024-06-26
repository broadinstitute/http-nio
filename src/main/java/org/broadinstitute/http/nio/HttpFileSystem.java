package org.broadinstitute.http.nio;

import org.broadinstitute.http.nio.utils.HttpUtils;
import org.broadinstitute.http.nio.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.Set;

/**
 * Read-only HTTP/S FileSystem.
 *
 * @author Daniel Gomez-Sanchez (magicDGS)
 */
final class HttpFileSystem extends FileSystem {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final HttpAbstractFileSystemProvider provider;

    // authority for this FileSystem
    private final String authority;

    /**
     * Construct a new FileSystem.
     *
     * @param provider  non {@code null} provider that generated this HTTP/S File System.
     * @param authority non {@code null} authority for this HTTP/S File System.
     */
    HttpFileSystem(final HttpAbstractFileSystemProvider provider, final String authority) {
        this.provider = Utils.nonNull(provider, () -> "null provider");
        this.authority = Utils.nonNull(authority, () -> "null authority");
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    /**
     * Gets the authority for this File System.
     *
     * @return the authority for this File System.
     */
    public String getAuthority() {
        return authority;
    }

    /**
     * This is a no-op, because {@link HttpFileSystem} is always open.
     *
     * @implNote because the open connections are not tracked, we cannot close the file system.
     */
    @Override
    public void close() {
        logger.warn("{} is always open (not closed)", this.getClass());
    }

    /**
     * {@link HttpFileSystem} is always open.
     *
     * @return {@code true}
     *
     * @implNote because the open connections are not tracked, we cannot close the file system.
     */
    @Override
    public boolean isOpen() {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code true}.
     */
    @Override
    public boolean isReadOnly() {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@link HttpUtils#HTTP_PATH_SEPARATOR_STRING}.
     */
    @Override
    public String getSeparator() {
        return HttpUtils.HTTP_PATH_SEPARATOR_STRING;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        // the root directory does not have the slash
        return Collections.singleton(new HttpPath(this, "", null, null));
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public HttpPath getPath(final String first, final String... more) {
        final String path = Utils.nonNull(first, () -> "null first")
                + String.join(getSeparator(), Utils.nonNull(more, () -> "null more"));

        if (!path.isEmpty() && !path.startsWith(getSeparator())) {
            throw new InvalidPathException(path, "Cannot construct a relative http/s path", 0);
        }

        try {
            // handle the Path with the URI to separate Path query and fragment
            // in addition, it checks for errors in the encoding (e.g., null chars)
            return getPath(new URI(path));
        } catch (URISyntaxException e) {
            throw new InvalidPathException(e.getInput(), e.getReason(), e.getIndex());
        }
    }


    /**
     * Gets the {@link HttpPath} from an {@link URI}.
     *
     * @param uri location of the HTTP/S resource.
     *
     * @return path representation of the {@link URI}.
     *
     * @implNote this method allows to pass the query and fragment to the {@link HttpPath}.
     */
    HttpPath getPath(final URI uri) {
        return new HttpPath(this, uri.getRawPath(), uri.getRawQuery(), uri.getRawFragment());
    }

    @Override
    public PathMatcher getPathMatcher(final String syntaxAndPattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public WatchService newWatchService() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String toString() {
        return String.format("%s[%s]@%s", this.getClass().getSimpleName(), provider, hashCode());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other instanceof HttpFileSystem) {
            final HttpFileSystem ofs = (HttpFileSystem) other;
            return provider() == ofs.provider() && getAuthority()
                    .equalsIgnoreCase(ofs.getAuthority());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 31 * provider.hashCode() + getAuthority().toLowerCase().hashCode();
    }
}
