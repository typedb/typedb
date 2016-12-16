/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graph.internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
public class ConceptLog {
    private Set<ConceptImpl> modifiedConcepts;
    private Set<CastingImpl> modifiedCastings;
    private Set<ResourceImpl> modifiedResources;
    private Map<String, RelationImpl> modifiedRelations;


    ConceptLog() {
        modifiedCastings = new HashSet<>();
        modifiedConcepts = new HashSet<>();
        modifiedResources = new HashSet<>();
        modifiedRelations = new HashMap<>();
    }

    /**
     * Removes all the concepts from the transaction tracker
     */
    public void clearTransaction(){
        modifiedConcepts.clear();
        modifiedCastings.clear();
        modifiedResources.clear();
        modifiedRelations.clear();
    }

    /**
     *
     * @param concept The concept to be later validated
     */
    public void putConcept(ConceptImpl concept) {
        if(!modifiedConcepts.contains(concept)) {
            modifiedConcepts.add(concept);

            if(concept.isCasting())
                modifiedCastings.add(concept.asCasting());
            if(concept.isResource())
                modifiedResources.add((ResourceImpl) concept);
        }

        //Caching of relations in memory so they can be retreived without needing a commit
        if(concept.isRelation()){
            RelationImpl relation = (RelationImpl) concept;
            modifiedRelations.put(RelationImpl.generateNewHash(relation.type(), relation.rolePlayers()), relation);
        }
    }

    /**
     *
     * @return All the concepts which have been affected within the transaction in some way
     */
    public Set<ConceptImpl> getModifiedConcepts () {
        modifiedConcepts = modifiedConcepts.stream().filter(c -> c != null && c.isAlive()).collect(Collectors.toSet());
        return modifiedConcepts;
    }

    /**
     *
     * @return All the castings which have been affected within the transaction in some way
     */
    public Set<String> getModifiedCastingIds() {
        return modifiedCastings.stream().filter(ConceptImpl::isAlive).map(concept -> concept.getBaseIdentifier().toString()).collect(Collectors.toSet());
    }

    /**
     *
     * @return All the castings which have been affected within the transaction in some way
     */
    public Set<String> getModifiedResourceIds() {
        return modifiedResources.stream().filter(ConceptImpl::isAlive).map(concept -> concept.getBaseIdentifier().toString()).collect(Collectors.toSet());
    }

    /**
     *
     * @param c The concept to nio longer track
     */
    public void removeConcept(ConceptImpl c){
        modifiedConcepts.remove(c);
        modifiedCastings.remove(c);
        modifiedResources.remove(c);
    }

    /**
     * Gets a cached relation by index. This way we can find non committed relations quickly.
     * @param index
     */
    public RelationImpl getCachedRelation(String index){
        return modifiedRelations.get(index);
    }

}
