package org.reactivesource.psql;

public class InvalidPayloadException extends RuntimeException {
    private static final long serialVersionUID = -3724701173093854056L;

    public InvalidPayloadException(String msg) {
        super(msg);
    }

    public InvalidPayloadException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
