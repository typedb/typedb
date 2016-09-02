/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.implementation;

import io.mindmaps.constants.DataType;

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
class ConceptLog {
    private Set<ConceptImpl> modifiedConcepts;
    private Set<ConceptImpl> modifiedCastings;

    ConceptLog() {
        modifiedCastings = new HashSet<>();
        modifiedConcepts = new HashSet<>();
    }

    /**
     * Removes all the concepts from the transaction tracker
     */
    public void clearTransaction(){
        modifiedConcepts.clear();
        modifiedCastings.clear();
    }

    /**
     *
     * @param concept The concept to be later validated
     */
    public void putConcept(ConceptImpl concept) {
        if(!modifiedConcepts.contains(concept)) {
            modifiedConcepts.add(concept);
            if (DataType.BaseType.CASTING.name().equals(concept.getBaseType()))
                modifiedCastings.add(concept);
        }
    }

    /**
     *
     * @return All the concepts which have been affected within the transaction in some way
     */
    public Set<ConceptImpl> getModifiedConcepts () {
        modifiedConcepts = modifiedConcepts.stream().filter(ConceptImpl::isAlive).collect(Collectors.toSet());
        return modifiedConcepts;
    }

    /**
     *
     * @return All the castings which have been affected within the transaction in some way
     */
    public Set<ConceptImpl> getModifiedCastings () {
        modifiedCastings = modifiedCastings.stream().filter(ConceptImpl::isAlive).collect(Collectors.toSet());
        return modifiedCastings;
    }

    /**
     *
     * @param c The concept to nio longer track
     */
    public void removeConcept(ConceptImpl c){
        modifiedConcepts.remove(c);
    }

}
