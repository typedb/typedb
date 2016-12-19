package ai.grakn.exception;

import ai.grakn.util.ErrorMessage;

/**
 * An exception which encapsulates a vendor backend error
 */
public class GraknBackendException extends GraphRuntimeException {
    public GraknBackendException(Exception e) {
        super(ErrorMessage.BACKEND_EXCEPTION.getMessage(), e);
    }
}
