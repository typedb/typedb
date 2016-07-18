package io.mindmaps.core.exceptions;

public class NoEdgeException extends GraphRuntimeException{
    public NoEdgeException(String id, String target) {
        super(ErrorMessage.NO_EDGE.getMessage(id, target));
    }
}
