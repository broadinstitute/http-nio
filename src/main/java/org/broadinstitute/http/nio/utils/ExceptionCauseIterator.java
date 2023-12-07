package org.broadinstitute.http.nio.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterate through the cause chain of a Throwable to examine all the causes in turn
 * Bounded by {@linkplain #MAX_DEPTH} in order to prevent infinite loops.
 */
public class ExceptionCauseIterator implements Iterator<Throwable>, Iterable<Throwable> {

    /**
     * The maximum depth of causes to explore before stopping
     */
    public static final int MAX_DEPTH = 20;

    private Throwable next;
    private int depth = 0;

    /**
     * @param ex the exception which will be iterated through
     */
    public ExceptionCauseIterator(Throwable ex) {
        next = ex;
    }

    @Override
    public boolean hasNext() {
        return next != null && depth < MAX_DEPTH;
    }

    @Override
    public Throwable next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Throwable tmp = next;
        next = tmp.getCause();
        depth++;
        return tmp;
    }

    @Override
    public Iterator<Throwable> iterator() {
        return this;
    }
}
