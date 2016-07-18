package io.mindmaps.core.exceptions;

import io.mindmaps.core.implementation.DataType;
import io.mindmaps.core.model.Concept;

public class MoreThanOneEdgeException extends GraphRuntimeException{
    public MoreThanOneEdgeException(Concept concept, DataType.EdgeLabel edgeType) {
        super(ErrorMessage.MORE_THAN_ONE_EDGE.getMessage(concept, edgeType.name()));
    }
}
