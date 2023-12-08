package org.broadinstitute.http.nio;

/**
 * indicates that a partial file was returned when the whole file was expecteted or vice a versa
 */
public class IncompatibleResponseToRangeQueryException extends UnexpectedHttpResponseException {

    /**
     * @param code the http response code recieved
     * @param msg  human readable error message
     */
    public IncompatibleResponseToRangeQueryException(int code, String msg) {
        super(code, msg);
    }
}
