package ai.grakn.exception;

import ai.grakn.util.ErrorMessage;

/**
 * A locking exception which may occur in a multi threaded environment.
 * Catching this exception, clearing the transaction, and retrying may allow the commit to execute successfully.
 */
public class GraknLockingException extends GraknBackendException {
    public GraknLockingException(Exception e) {
        super(ErrorMessage.LOCKING_EXCEPTION.getMessage(), e);
    }
}
