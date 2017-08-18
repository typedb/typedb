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

package ai.grakn.kb.internal.cache;

import ai.grakn.GraknTxType;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Label;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Thing;
import ai.grakn.kb.internal.concept.SchemaConceptImpl;
import ai.grakn.kb.internal.concept.RelationshipReified;
import ai.grakn.kb.internal.concept.ThingImpl;
import ai.grakn.kb.internal.structure.Casting;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import mjson.Json;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 *     Tracks Graph Transaction Specific Variables
 * </p>
 *
 * <p>
 *     Caches Transaction specific data this includes:
 *     <ol>
 *         <li>Validation Concepts - Concepts which need to undergo validation.</li>
 *         <li>Built Concepts -  Prevents rebuilding when the same vertex is encountered</li>
 *         <li>The Schema - Optimises validation checks by preventing db read. </li>
 *         <li> {@link Label} - Allows mapping type labels to type Ids</li>
 *         <li>Transaction meta Data - Allows transactions to function in different ways</li>
 *     <ol/>
 * </p>
 *
 * @author fppt
 *
 */
public class TxCache {
    //Graph cache which is shared across multiple transactions
    private final GraphCache graphCache;

    //Caches any concept which has been touched before
    private final Map<ConceptId, Concept> conceptCache = new HashMap<>();
    private final Map<Label, SchemaConcept> schemaConceptCache = new HashMap<>();
    private final Map<Label, LabelId> labelCache = new HashMap<>();

    //Elements Tracked For Validation
    private final Set<Entity> modifiedEntities = new HashSet<>();

    private final Set<Role> modifiedRoles = new HashSet<>();
    private final Set<Casting> modifiedCastings = new HashSet<>();

    private final Set<RelationshipType> modifiedRelationshipTypes = new HashSet<>();
    private final Set<Relationship> modifiedRelationships = new HashSet<>();

    private final Set<Rule> modifiedRules = new HashSet<>();

    private final Set<Attribute> modifiedAttributes = new HashSet<>();

    //We Track Relations so that we can look them up before they are completely defined and indexed on commit
    private final Map<String, Relationship> relationIndexCache = new HashMap<>();

    //We Track the number of concept connections which have been made which may result in a new shard
    private final Map<ConceptId, Long> shardingCount = new HashMap<>();

    //Transaction Specific Meta Data
    private boolean isTxOpen = false;
    private GraknTxType txType;
    private String closedReason = null;

    public TxCache(GraphCache graphCache) {
        this.graphCache = graphCache;
    }

    /**
     * A helper method which writes back into the graph cache at the end of a transaction.
     *
     * @param isSafe true only if it is safe to copy the cache completely without any checks
     */
    public void writeToGraphCache(boolean isSafe){
        //When a commit has occurred or a graph is read only all types can be overridden this is because we know they are valid.
        if(isSafe) graphCache.readTxCache(this);

        //When a commit has not occurred some checks are required
        //TODO: Fill our cache when not committing and when not read only graph.
    }

    /**
     *
     * @return true if ths schema labels have been cached. The graph cannot operate if this is false.
     */
    public boolean schemaNotCached(){
        return labelCache.isEmpty();
    }

    /**
     * Refreshes the transaction schema cache by reading the central schema cache is read into this transaction cache.
     * This method performs this operation whilst making a deep clone of the cached concepts to ensure transactions
     * do not accidentally break the central schema cache.
     *
     */
    public void refreshSchemaCache(){
        Map<Label, SchemaConcept> cachedSchemaSnapshot = graphCache.getCachedTypes();
        Map<Label, LabelId> cachedLabelsSnapshot = graphCache.getCachedLabels();

        //Read central cache into txCache cloning only base concepts. Sets clones later
        for (SchemaConcept type : cachedSchemaSnapshot.values()) {
            cacheConcept(type);
        }

        //Load Labels Separately. We do this because the TypeCache may have expired.
        cachedLabelsSnapshot.forEach(this::cacheLabel);
    }

    /**
     *
     * @param concept The element to be later validated
     */
    public void trackForValidation(Concept concept) {
        if (concept.isEntity()) {
            modifiedEntities.add(concept.asEntity());
        } else if (concept.isRole()) {
            modifiedRoles.add(concept.asRole());
        } else if (concept.isRelationshipType()) {
            modifiedRelationshipTypes.add(concept.asRelationshipType());
        } else if (concept.isRelationship()){
            Relationship relationship = concept.asRelationship();
            modifiedRelationships.add(relationship);
            //Caching of relations in memory so they can be retrieved without needing a commit
            relationIndexCache.put(RelationshipReified.generateNewHash(relationship.type(), relationship.allRolePlayers()), relationship);
        } else if (concept.isRule()){
            modifiedRules.add(concept.asRule());
        } else if (concept.isAttribute()){
            modifiedAttributes.add(concept.asAttribute());
        }
    }
    public void trackForValidation(Casting casting) {
        modifiedCastings.add(casting);
    }

    /**
     *
     * @return All the relations which have been affected in the transaction
     */
    public Map<String, Relationship> getRelationIndexCache(){
        return relationIndexCache;
    }

    /**
     *
     * @return All the types that have gained or lost instances and by how much
     */
    public Map<ConceptId, Long> getShardingCount(){
        return shardingCount;
    }

    /**
     *
     * @return All the types currently cached in the transaction. Used for
     */
    Map<Label, SchemaConcept> getSchemaConceptCache(){
        return schemaConceptCache;
    }

    /**
     *
     * @return All the types labels currently cached in the transaction.
     */
    Map<Label, LabelId> getLabelCache(){
        return labelCache;
    }

    /**
     *
     * @return All the concepts which have been accessed in this transaction
     */
    Map<ConceptId, Concept> getConceptCache() {
        return conceptCache;
    }

    /**
     *
     * @param concept The concept to no longer track
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public void remove(Concept concept){
        modifiedEntities.remove(concept);
        modifiedRoles.remove(concept);
        modifiedRelationshipTypes.remove(concept);
        modifiedRelationships.remove(concept);
        modifiedRules.remove(concept);
        modifiedAttributes.remove(concept);

        conceptCache.remove(concept.getId());
        if (concept.isSchemaConcept()) {
            Label label = ((SchemaConceptImpl) concept).getLabel();
            schemaConceptCache.remove(label);
            labelCache.remove(label);
        }
    }

    /**
     * Gets a cached relation by index. This way we can find non committed relations quickly.
     *
     * @param index The current index of the relation
     */
    public Relationship getCachedRelation(String index){
        return relationIndexCache.get(index);
    }

    /**
     * Caches a concept so it does not have to be rebuilt later.
     *
     * @param concept The concept to be cached.
     */
    public void cacheConcept(Concept concept){
        conceptCache.put(concept.getId(), concept);
        if(concept.isSchemaConcept()){
            SchemaConceptImpl schemaConcept = (SchemaConceptImpl) concept;
            schemaConceptCache.put(schemaConcept.getLabel(), schemaConcept);
            labelCache.put(schemaConcept.getLabel(), schemaConcept.getLabelId());
        }
    }


    /**
     * Caches the mapping of a type label to a type id. This is necessary in order for ANY types to be looked up.
     *
     * @param label The type label to cache
     * @param id Its equivalent id which can be looked up quickly in the graph
     */
    private void cacheLabel(Label label, LabelId id){
        labelCache.put(label, id);
    }

    /**
     * Checks if the concept has been built before and is currently cached
     *
     * @param id The id of the concept
     * @return true if the concept is cached
     */
    public boolean isConceptCached(ConceptId id){
        return conceptCache.containsKey(id);
    }

    /**
     *
     * @param label The label of the type to cache
     * @return true if the concept is cached
     */
    public boolean isTypeCached(Label label){
        return schemaConceptCache.containsKey(label);
    }

    /**
     *
     * @param label the type label which may be in the cache
     * @return true if the label is cached and has a valid mapping to a id
     */
    public boolean isLabelCached(Label label){
        return labelCache.containsKey(label);
    }

    /**
     * Returns a previously built concept
     *
     * @param id The id of the concept
     * @param <X> The type of the concept
     * @return The cached concept
     */
    public <X extends Concept> X getCachedConcept(ConceptId id){
        //noinspection unchecked
        return (X) conceptCache.get(id);
    }

    /**
     * Returns a previously built type
     *
     * @param label The label of the type
     * @param <X> The type of the type
     * @return The cached type
     */
    public <X extends SchemaConcept> X getCachedSchemaConcept(Label label){
        //noinspection unchecked
        return (X) schemaConceptCache.get(label);
    }

    public LabelId convertLabelToId(Label label){
        return labelCache.get(label);
    }

    public void addedInstance(ConceptId conceptId){
        shardingCount.compute(conceptId, (key, value) -> value == null ? 1 : value + 1);
        cleanupShardingCount(conceptId);
    }
    public void removedInstance(ConceptId conceptId){
        shardingCount.compute(conceptId, (key, value) -> value == null ? -1 : value - 1);
        cleanupShardingCount(conceptId);
    }
    private void cleanupShardingCount(ConceptId conceptId){
        if(shardingCount.get(conceptId) == 0) shardingCount.remove(conceptId);
    }

    public Json getFormattedLog(){
        //Concepts In Need of Inspection
        Json conceptsForInspection = Json.object();
        conceptsForInspection.set(Schema.BaseType.ATTRIBUTE.name(), loadConceptsForFixing(getModifiedAttributes()));

        //Types with instance changes
        Json typesWithInstanceChanges = Json.array();

        getShardingCount().forEach((key, value) -> {
            Json jsonObject = Json.object();
            jsonObject.set(REST.Request.COMMIT_LOG_CONCEPT_ID, key.getValue());
            jsonObject.set(REST.Request.COMMIT_LOG_SHARDING_COUNT, value);
            typesWithInstanceChanges.add(jsonObject);
        });

        //Final Commit Log
        Json formattedLog = Json.object();
        formattedLog.set(REST.Request.COMMIT_LOG_FIXING, conceptsForInspection);
        formattedLog.set(REST.Request.COMMIT_LOG_COUNTING, typesWithInstanceChanges);

        return formattedLog;
    }
    private  <X extends Thing> Json loadConceptsForFixing(Set<X> instances){
        Map<String, Set<String>> conceptByIndex = new HashMap<>();
        instances.forEach(thing ->
                conceptByIndex.computeIfAbsent(((ThingImpl) thing).getIndex(), (e) -> new HashSet<>()).add(thing.getId().getValue()));
        return Json.make(conceptByIndex);
    }

    //--------------------------------------- Concepts Needed For Validation -------------------------------------------
    public Set<Entity> getModifiedEntities() {
        return modifiedEntities;
    }

    public Set<Role> getModifiedRoles() {
        return modifiedRoles;
    }

    public Set<RelationshipType> getModifiedRelationshipTypes() {
        return modifiedRelationshipTypes;
    }
    public Set<Relationship> getModifiedRelationships() {
        return modifiedRelationships;
    }

    public Set<Rule> getModifiedRules() {
        return modifiedRules;
    }

    public Set<Attribute> getModifiedAttributes() {
        return modifiedAttributes;
    }

    public Set<Casting> getModifiedCastings() {
        return modifiedCastings;
    }

    //--------------------------------------- Transaction Specific Meta Data -------------------------------------------
    public void closeTx(String closedReason){
        isTxOpen = false;
        this.closedReason = closedReason;

        //Clear Concept Caches
        conceptCache.values().forEach(concept -> ContainsTxCache.from(concept).txCacheClear());

        //Clear Collection Caches
        modifiedEntities.clear();
        modifiedRoles.clear();
        modifiedRelationshipTypes.clear();
        modifiedRelationships.clear();
        modifiedRules.clear();
        modifiedAttributes.clear();
        modifiedCastings.clear();
        relationIndexCache.clear();
        shardingCount.clear();
        conceptCache.clear();
        schemaConceptCache.clear();
        labelCache.clear();
    }
    public void openTx(GraknTxType txType){
        isTxOpen = true;
        this.txType = txType;
        closedReason = null;
    }
    public boolean isTxOpen(){
        return isTxOpen;
    }

    public GraknTxType txType(){
        return txType;
    }

    public String getClosedReason(){
        return closedReason;
    }
}
