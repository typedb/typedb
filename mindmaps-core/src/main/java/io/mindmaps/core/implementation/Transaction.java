package io.mindmaps.core.implementation;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 *  This package keeps track of changes to the rootGraph that need to be validated. This includes:
 *      new concepts,
 *      concepts that have had edges added/deleted,
 *      edge cases, for example, relationship where a new role player is added.
 *
 */

class Transaction {
    private Set<ConceptImpl> modifiedConcepts;
    private Set<ConceptImpl> modifiedCastings;
    private Set<ConceptImpl> modifiedRelations;

    Transaction () {
        modifiedCastings = new HashSet<>();
        modifiedConcepts = new HashSet<>();
        modifiedRelations = new HashSet<>();
    }

    public void clearTransaction(){
        modifiedConcepts.clear();
        modifiedCastings.clear();
        modifiedRelations.clear();
    }

    public void putConcept(ConceptImpl concept) {
        if(!modifiedConcepts.contains(concept)) {
            modifiedConcepts.add(concept);
            if (DataType.BaseType.RELATION.name().equals(concept.getBaseType()))
                modifiedRelations.add(concept);
            else if (DataType.BaseType.CASTING.name().equals(concept.getBaseType()))
                modifiedCastings.add(concept);
        }
    }

    public Set<ConceptImpl> getModifiedConcepts () {
        modifiedConcepts = modifiedConcepts.stream().filter(ConceptImpl::isAlive).collect(Collectors.toSet());
        return modifiedConcepts;
    }

    public Set<ConceptImpl> getModifiedCastings () {
        modifiedCastings = modifiedCastings.stream().filter(ConceptImpl::isAlive).collect(Collectors.toSet());
        return modifiedCastings;
    }

    public Set<ConceptImpl> getModifiedRelations () {
        modifiedRelations = modifiedRelations.stream().filter(ConceptImpl::isAlive).collect(Collectors.toSet());
        return modifiedRelations;
    }

    public void removeConcept(ConceptImpl c){
        modifiedConcepts.remove(c);
    }

}
