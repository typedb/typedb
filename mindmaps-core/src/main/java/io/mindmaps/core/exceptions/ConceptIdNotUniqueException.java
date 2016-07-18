package io.mindmaps.core.exceptions;

import io.mindmaps.core.implementation.DataType;
import io.mindmaps.core.model.Concept;

public class ConceptIdNotUniqueException extends ConceptException {
    public ConceptIdNotUniqueException(Concept concept, DataType.ConceptPropertyUnique type, String id) {
        super(ErrorMessage.ID_NOT_UNIQUE.getMessage(concept.toString(), type.name(), id));
    }

    public ConceptIdNotUniqueException(Concept concept, String id){
        super(ErrorMessage.ID_ALREADY_TAKEN.getMessage(id, concept.toString()));
    }
}
