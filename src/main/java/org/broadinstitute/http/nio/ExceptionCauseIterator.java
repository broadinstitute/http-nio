package org.broadinstitute.http.nio;

import java.util.Iterator;
import java.util.NoSuchElementException;

class ExceptionCauseIterator implements Iterator<Throwable>, Iterable<Throwable> {
    private Throwable next;
    private static final int MAX_DEPTH = 20;
    private int depth = 0;

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
