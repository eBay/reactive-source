package org.reactivesource.exceptions;

public class ConfigurationException extends RuntimeException {
    private static final long serialVersionUID = -7104365629425825102L;

    public ConfigurationException() {
        super();
    }

    public ConfigurationException(String msg) {
        super(msg);
    }

    public ConfigurationException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
