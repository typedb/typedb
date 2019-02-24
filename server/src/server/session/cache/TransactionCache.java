/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.session.cache;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Label;
import grakn.core.concept.LabelId;
import grakn.core.concept.Relation;
import grakn.core.concept.RelationType;
import grakn.core.concept.Role;
import grakn.core.concept.Rule;
import grakn.core.concept.SchemaConcept;
import grakn.core.concept.Thing;
import grakn.core.concept.Type;
import grakn.core.server.Transaction;
import grakn.core.server.kb.cache.CacheOwner;
import grakn.core.server.kb.concept.AttributeImpl;
import grakn.core.server.kb.structure.Casting;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Caches Transaction specific data this includes:
 * Validation Concepts - Concepts which need to undergo validation.
 * Built Concepts -  Prevents rebuilding when the same vertex is encountered
 * The Schema - Optimises validation checks by preventing db read.
 * {@link Label} - Allows mapping type labels to type Ids
 * Transaction meta Data - Allows transactions to function in different ways
 */
public class TransactionCache {
    //Cache which is shared across multiple transactions
    private final GlobalCache globalCache;

    //Caches any concept which has been touched before
    private final Map<ConceptId, Concept> conceptCache = new HashMap<>();
    private final Map<Label, SchemaConcept> schemaConceptCache = new HashMap<>();
    private final Map<Label, LabelId> labelCache = new HashMap<>();

    //Elements Tracked For Validation
    private final Set<Relation> newRelationships = new HashSet<>();
    private final Set<Thing> modifiedThings = new HashSet<>();

    private final Set<Role> modifiedRoles = new HashSet<>();
    private final Set<Casting> modifiedCastings = new HashSet<>();

    private final Set<RelationType> modifiedRelationshipTypes = new HashSet<>();

    private final Set<Rule> modifiedRules = new HashSet<>();

    //We Track the number of concept connections which have been made which may result in a new shard
    private final Map<ConceptId, Long> shardingCount = new HashMap<>();

    //New attributes are tracked so that we can merge any duplicate attributes in post.
    // This is a map of attribute indices to concept ids
    // The index and id are directly cached to prevent unneeded reads
    private Multimap<String, ConceptId> newAttributes = ArrayListMultimap.create();

    //Transaction Specific Meta Data
    private boolean isTxOpen = false;
    private boolean writeOccurred = false;
    private Transaction.Type txType;
    private String closedReason = null;

    public TransactionCache(GlobalCache globalCache) {
        this.globalCache = globalCache;
    }

    /**
     * A helper method which writes back into the graph cache at the end of a transaction.
     *
     * @param isSafe true only if it is safe to copy the cache completely without any checks
     */
    public void writeToGraphCache(boolean isSafe) {
        //When a commit has occurred or a transaction is read only all types can be overridden this is because we know they are valid.
        //When it is not safe to simply flush we have to check that no mutations were made
        if (isSafe || !writeOccurred) {
            globalCache.readTxCache(this);
        }
    }

    /**
     * Notifies the cache that a write has occurred.
     * This is later used to determine if it is safe to flush the transaction cache to the session cache or not.
     */
    public void writeOccurred() {
        writeOccurred = true;
    }

    /**
     * @return true if ths schema labels have been cached. The graph cannot operate if this is false.
     */
    public boolean schemaNotCached() {
        return labelCache.isEmpty();
    }

    /**
     * Refreshes the transaction schema cache by reading the central schema cache is read into this transaction cache.
     * This method performs this operation whilst making a deep clone of the cached concepts to ensure transactions
     * do not accidentally break the central schema cache.
     */
    public void refreshSchemaCache() {
        globalCache.populateSchemaTxCache(this);
    }

    /**
     * @param concept The element to be later validated
     */
    public void trackForValidation(Concept concept) {
        if (concept.isThing()) {
            modifiedThings.add(concept.asThing());
        } else if (concept.isRole()) {
            modifiedRoles.add(concept.asRole());
        } else if (concept.isRelationType()) {
            modifiedRelationshipTypes.add(concept.asRelationType());
        } else if (concept.isRule()) {
            modifiedRules.add(concept.asRule());
        }
    }

    public void trackForValidation(Casting casting) {
        modifiedCastings.add(casting);
    }

    public void removeFromValidation(Type type) {
        if (type.isRelationType()) {
            modifiedRelationshipTypes.remove(type.asRelationType());
        }
    }

    /**
     * @return All the types that have gained or lost instances and by how much
     */
    public Map<ConceptId, Long> getShardingCount() {
        return shardingCount;
    }

    /**
     * @return All the types currently cached in the transaction. Used for
     */
    Map<Label, SchemaConcept> getSchemaConceptCache() {
        return schemaConceptCache;
    }

    /**
     * @return All the types labels currently cached in the transaction.
     */
    Map<Label, LabelId> getLabelCache() {
        return labelCache;
    }

    /**
     * @return All the concepts which have been accessed in this transaction
     */
    Map<ConceptId, Concept> getConceptCache() {
        return conceptCache;
    }

    /**
     * @param concept The concept to no longer track
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public void remove(Concept concept) {
        modifiedThings.remove(concept);
        modifiedRoles.remove(concept);
        modifiedRelationshipTypes.remove(concept);
        modifiedRules.remove(concept);

        if (concept.isAttribute()) {
            newAttributes.removeAll(AttributeImpl.from(concept.asAttribute()).getIndex());
        }

        if (concept.isRelation()) {
            newRelationships.remove(concept.asRelation());
        }

        conceptCache.remove(concept.id());
        if (concept.isSchemaConcept()) {
            Label label = concept.asSchemaConcept().label();
            schemaConceptCache.remove(label);
            labelCache.remove(label);
        }
    }

    public void remove(Casting casting) {
        modifiedCastings.remove(casting);
    }

    /**
     * Caches a concept so it does not have to be rebuilt later.
     *
     * @param concept The concept to be cached.
     */
    public void cacheConcept(Concept concept) {
        conceptCache.put(concept.id(), concept);
        if (concept.isSchemaConcept()) {
            SchemaConcept schemaConcept = concept.asSchemaConcept();
            schemaConceptCache.put(schemaConcept.label(), schemaConcept);
            labelCache.put(schemaConcept.label(), schemaConcept.labelId());
        }
    }


    /**
     * Caches the mapping of a type label to a type id. This is necessary in order for ANY types to be looked up.
     *
     * @param label The type label to cache
     * @param id    Its equivalent id which can be looked up quickly in the graph
     */
    void cacheLabel(Label label, LabelId id) {
        labelCache.put(label, id);
    }

    /**
     * Checks if the concept has been built before and is currently cached
     *
     * @param id The id of the concept
     * @return true if the concept is cached
     */
    public boolean isConceptCached(ConceptId id) {
        return conceptCache.containsKey(id);
    }

    /**
     * @param label The label of the type to cache
     * @return true if the concept is cached
     */
    public boolean isTypeCached(Label label) {
        return schemaConceptCache.containsKey(label);
    }

    /**
     * @param label the type label which may be in the cache
     * @return true if the label is cached and has a valid mapping to a id
     */
    public boolean isLabelCached(Label label) {
        return labelCache.containsKey(label);
    }

    /**
     * Returns a previously built concept
     *
     * @param id  The id of the concept
     * @param <X> The type of the concept
     * @return The cached concept
     */
    public <X extends Concept> X getCachedConcept(ConceptId id) {
        //noinspection unchecked
        return (X) conceptCache.get(id);
    }

    /**
     * Returns a previously built type
     *
     * @param label The label of the type
     * @param <X>   The type of the type
     * @return The cached type
     */
    public <X extends SchemaConcept> X getCachedSchemaConcept(Label label) {
        //noinspection unchecked
        return (X) schemaConceptCache.get(label);
    }

    public LabelId convertLabelToId(Label label) {
        return labelCache.get(label);
    }

    public void addedInstance(ConceptId conceptId) {
        shardingCount.compute(conceptId, (key, value) -> value == null ? 1 : value + 1);
        cleanupShardingCount(conceptId);
    }

    public void removedInstance(ConceptId conceptId) {
        shardingCount.compute(conceptId, (key, value) -> value == null ? -1 : value - 1);
        cleanupShardingCount(conceptId);
    }

    private void cleanupShardingCount(ConceptId conceptId) {
        if (shardingCount.get(conceptId) == 0) shardingCount.remove(conceptId);
    }


    public void addNewAttribute(String index, ConceptId conceptId) {
        newAttributes.put(index, conceptId);
    }

    public Map<String, Set<ConceptId>> getNewAttributes() {
        Map<String, Set<ConceptId>> map = new HashMap<>();
        newAttributes.asMap().forEach((attrValue, conceptIds) -> map.put(attrValue, new HashSet<>(conceptIds)));
        return map;
    }

    //--------------------------------------- Concepts Needed For Validation -------------------------------------------
    public Set<Thing> getModifiedThings() {
        return modifiedThings;
    }

    public Set<Role> getModifiedRoles() {
        return modifiedRoles;
    }

    public Set<RelationType> getModifiedRelationshipTypes() {
        return modifiedRelationshipTypes;
    }

    public Set<Rule> getModifiedRules() {
        return modifiedRules;
    }

    public Set<Casting> getModifiedCastings() {
        return modifiedCastings;
    }

    public void addNewRelationship(Relation relationship) {
        newRelationships.add(relationship);
    }

    public Set<Relation> getNewRelationships() {
        return newRelationships;
    }

    //--------------------------------------- Transaction Specific Meta Data -------------------------------------------
    public void closeTx(String closedReason) {
        isTxOpen = false;
        this.closedReason = closedReason;

        //Clear Concept Caches
        conceptCache.values().forEach(concept -> CacheOwner.from(concept).txCacheClear());

        //Clear Collection Caches
        modifiedThings.clear();
        modifiedRoles.clear();
        modifiedRelationshipTypes.clear();
        modifiedRules.clear();
        modifiedCastings.clear();
        newAttributes.clear();
        newRelationships.clear();
        shardingCount.clear();
        conceptCache.clear();
        schemaConceptCache.clear();
        labelCache.clear();
    }

    public void open(Transaction.Type type) {
        isTxOpen = true;
        this.txType = type;
        closedReason = null;
    }

    public boolean isTxOpen() {
        return isTxOpen;
    }

    public Transaction.Type txType() {
        return txType;
    }

    public String getClosedReason() {
        return closedReason;
    }

}
