package org.broadinstitute.http.nio.utils;

import java.util.function.Supplier;

/**
 * Generic utility methods.
 *
 * @author Daniel Gomez-Sanchez (magicDGS)
 */
public class Utils {

    private Utils() {}

    /**
     * Throws an {@link IllegalArgumentException} if the object is {@code null}.
     *
     * @param o   object to test.
     * @param msg message for the exception.
     * @param <T> type of the object.
     *
     * @return the same object if not {@code null}.
     *
     * @throws IllegalArgumentException if the object is {@code null}.
     */
    public static <T> T nonNull(T o, Supplier<String> msg) {
        if (o == null) {
            throw new IllegalArgumentException(msg.get());
        }
        return o;
    }

    /**
     * @param condition throw an Illegal argument exception if !condition
     * @param msg the error message when the condition is not met
     */
    public static void validateArg(final boolean condition, final String msg){
        if (!condition){
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * {@link RuntimeException} for parts of the code that should not happen.
     */
    public static class ShouldNotHappenException extends RuntimeException {

        /**
         * Constructor.
         *
         * @param e cause. May be {@code null}.
         */
        public ShouldNotHappenException(final Throwable e) {
            super("Should not happen", e);
        }

        /**
         * Constructor.
         * @param msg the message to display
         */
        public ShouldNotHappenException(final String msg) {
            super(msg);
        }
    }
}
