package net.plumbing.msgbus.ws.client;

import net.plumbing.msgbus.ws.SoapException;

/**
 * Top-level exception type thrown by SoapClient
 *
 * @author Tom Bujok
 * @since 1.0.0
 */
public class SoapClientException extends SoapException {
    public SoapClientException(String message) {
        super(message);
    }

    public SoapClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public SoapClientException(Throwable cause) {
        super(cause);
    }
}
