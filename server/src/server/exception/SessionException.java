package grakn.core.server.exception;

import grakn.core.common.exception.GraknException;

public class SessionException extends GraknException {

    public SessionException(String message) {
        super(message);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }
}
