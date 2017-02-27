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

import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 *     Tracks Graph Mutations.
 * </p>
 *
 * <p>
 *     This package keeps track of changes to the rootGraph that need to be validated. This includes:
 *      new concepts,
 *      concepts that have had edges added/deleted,
 *      edge cases, for example, relationship where a new role player is added.
 * </p>
 *
 * @author fppt
 *
 */
class ConceptLog {
    private final AbstractGraknGraph<?> graknGraph;

    //Caches any concept which has been touched before
    private final Map<ConceptId, ConceptImpl> conceptCache = new HashMap<>();
    private final Map<TypeName, TypeImpl> typeCache = new HashMap<>();

    //We Track Modified Concepts For Validation
    private final Set<ConceptImpl> modifiedConcepts = new HashSet<>();

    //We Track Casting Explicitly For Post Processing
    private final Set<CastingImpl> modifiedCastings = new HashSet<>();

    //We Track Resource Explicitly for Post Processing
    private final Set<ResourceImpl> modifiedResources = new HashSet<>();

    //We Track Relations so that we can look them up before they are completely defined and indexed on commit
    private final Map<String, RelationImpl> modifiedRelations = new HashMap<>();


    ConceptLog(AbstractGraknGraph<?> graknGraph) {
        this.graknGraph = graknGraph;
    }

    /**
     * A helper method which writes back into the central cache at the end of a transaction.
     *
     * @param committed true if a commit has occurred
     */
    void writeToCentralCache(boolean committed){
        //When a commit has occurred all types can be overridden this is because we know they are valid
        //If a commit has not occurred we can only safely push types to the central cache if no modifications have occurred.
        if(committed){
            graknGraph.getCachedOntology().putAll(typeCache);
        }

        //TODO: Fill our cache when not committing
    }

    /**
     *
     * @param concept The concept to be later validated
     */
    void trackConceptForValidation(ConceptImpl concept) {
        if (!modifiedConcepts.contains(concept)) {
            modifiedConcepts.add(concept);

            if (concept.isCasting()) {
                modifiedCastings.add(concept.asCasting());
            }
            if (concept.isResource()) {
                modifiedResources.add((ResourceImpl) concept);
            }
        }

        //Caching of relations in memory so they can be retrieved without needing a commit
        if (concept.isRelation()) {
            RelationImpl relation = (RelationImpl) concept;
            modifiedRelations.put(RelationImpl.generateNewHash(relation.type(), relation.rolePlayers()), relation);
        }
    }

    /**
     *
     * @return All the concepts which have been affected within the transaction in some way
     */
    Set<ConceptImpl> getModifiedConcepts() {
        return modifiedConcepts;
    }

    /**
     *
     * @return All the castings which have been affected within the transaction in some way
     */
    Set<CastingImpl> getModifiedCastings() {
        return modifiedCastings;
    }

    /**
     *
     * @return All the castings which have been affected within the transaction in some way
     */
    Set<ResourceImpl> getModifiedResources() {
        return modifiedResources;
    }

    /**
     *
     * @return All the relations which have been affected in the transaction
     */
    Map<String, RelationImpl> getModifiedRelations(){
        return modifiedRelations;
    }

    /**
     *
     * @param concept The concept to nio longer track
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    void removeConcept(ConceptImpl concept){
        modifiedConcepts.remove(concept);
        modifiedCastings.remove(concept);
        modifiedResources.remove(concept);
        conceptCache.remove(concept.getId());
        if(concept.isType()){
            typeCache.remove(((TypeImpl) concept).getName());
        }
    }

    /**
     * Gets a cached relation by index. This way we can find non committed relations quickly.
     *
     * @param index The current index of the relation
     */
    RelationImpl getCachedRelation(String index){
        return modifiedRelations.get(index);
    }

    /**
     * Caches a concept so it does not have to be rebuilt later.
     *
     * @param concept The concept to be cached.
     */
    void cacheConcept(ConceptImpl concept){
        conceptCache.put(concept.getId(), concept);
        if(concept.isType()){
            TypeImpl type = (TypeImpl) concept;
            typeCache.put(type.getName(), type);
        }
    }

    /**
     * Checks if the concept has been built before and is currently cached
     *
     * @param id The id of the concept
     * @return true if the concept is cached
     */
    boolean isConceptCached(ConceptId id){
        return conceptCache.containsKey(id);
    }

    /**
     *
     * @param name The name of the type to cache
     * @return true if the concept is cached
     */
    boolean isTypeCached(TypeName name){
        return typeCache.containsKey(name);
    }

    /**
     * Returns a previously built concept
     *
     * @param id The id of the concept
     * @param <X> The type of the concept
     * @return The cached concept
     */
    <X extends Concept> X getCachedConcept(ConceptId id){
        //noinspection unchecked
        return (X) conceptCache.get(id);
    }

    /**
     * Returns a previously built type
     *
     * @param name The name of the type
     * @param <X> The type of the type
     * @return The cached type
     */
    <X extends Type> X getCachedType(TypeName name){
        //noinspection unchecked
        return (X) typeCache.get(name);
    }
}
