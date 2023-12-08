package org.broadinstitute.http.nio;

import java.io.IOException;

/**
 * thrown when we recieve an unexpected response code from an http request which is not otherwise specially handled
 */
public class UnexpectedHttpResponseException extends IOException {

    /** http response code */
    private final int responseCode;

    /**
     * @param responseCode the http response code recieved
     * @param msg          human readable error message
     */
    public UnexpectedHttpResponseException(int responseCode, String msg) {
        super(msg);
        this.responseCode = responseCode;
    }

    /**
     * @return the http response code
     */
    public int getResponseCode() {
        return responseCode;
    }
}
