package io.mindmaps.core.exceptions;

import io.mindmaps.core.model.Concept;

public class InvalidConceptTypeException extends ConceptException {
    public InvalidConceptTypeException(Concept c, Class type) {
        super(ErrorMessage.INVALID_OBJECT_TYPE.getMessage(c.toString(), type.getName()));
    }
}
