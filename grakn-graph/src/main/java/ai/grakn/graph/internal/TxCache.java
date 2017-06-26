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

import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.concept.Type;
import ai.grakn.concept.LabelId;
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
 *         <li>The Ontology - Optimises validation checks by preventing db read. </li>
 *         <li>Type Labels - Allows mapping type labels to type Ids</li>
 *         <li>Transaction meta Data - Allows transactions to function in different ways</li>
 *     <ol/>
 * </p>
 *
 * @author fppt
 *
 */
class TxCache {
    //Graph cache which is shared across multiple transactions
    private final GraphCache graphCache;

    //Caches any concept which has been touched before
    private final Map<ConceptId, ConceptImpl> conceptCache = new HashMap<>();
    private final Map<Label, TypeImpl> typeCache = new HashMap<>();
    private final Map<Label, LabelId> labelCache = new HashMap<>();

    //Elements Tracked For Validation
    private final Set<EntityImpl> modifiedEntities = new HashSet<>();

    private final Set<RoleTypeImpl> modifiedRoleTypes = new HashSet<>();
    private final Set<Casting> modifiedCastings = new HashSet<>();

    private final Set<RelationTypeImpl> modifiedRelationTypes = new HashSet<>();
    private final Set<RelationImpl> modifiedRelations = new HashSet<>();

    private final Set<RuleImpl> modifiedRules = new HashSet<>();

    private final Set<ResourceImpl> modifiedResources = new HashSet<>();

    //We Track Relations so that we can look them up before they are completely defined and indexed on commit
    private final Map<String, RelationImpl> relationIndexCache = new HashMap<>();

    //We Track the number of concept connections which have been made which may result in a new shard
    private final Map<ConceptId, Long> shardingCount = new HashMap<>();

    //Transaction Specific Meta Data
    private boolean isTxOpen = false;
    private boolean showImplicitTypes = false;
    private GraknTxType txType;
    private String closedReason = null;

    TxCache(GraphCache graphCache) {
        this.graphCache = graphCache;
    }

    /**
     * A helper method which writes back into the graph cache at the end of a transaction.
     *
     * @param isSafe true only if it is safe to copy the cache completely without any checks
     */
    void writeToGraphCache(boolean isSafe){
        //When a commit has occurred or a graph is read only all types can be overridden this is because we know they are valid.
        if(isSafe) graphCache.readTxCache(this);

        //When a commit has not occurred some checks are required
        //TODO: Fill our cache when not committing and when not read only graph.
    }

    /**
     *
     * @return true if ths ontology labels have been cached. The graph cannot operate if this is false.
     */
    boolean ontologyNotCached(){
        return labelCache.isEmpty();
    }

    /**
     * Refreshes the transaction ontology cache by reading the central ontology cache is read into this transaction cache.
     * This method performs this operation whilst making a deep clone of the cached concepts to ensure transactions
     * do not accidentally break the central ontology cache.
     *
     */
    void refreshOntologyCache(){
        Map<Label, Type> cachedOntologySnapshot = graphCache.getCachedTypes();
        Map<Label, LabelId> cachedLabelsSnapshot = graphCache.getCachedLabels();

        //Read central cache into txCache cloning only base concepts. Sets clones later
        for (Type type : cachedOntologySnapshot.values()) {
            cacheConcept((TypeImpl) type);
        }

        //Load Labels Separately. We do this because the TypeCache may have expired.
        cachedLabelsSnapshot.forEach(this::cacheLabel);
    }

    /**
     *
     * @param element The element to be later validated
     */
    void trackForValidation(ConceptImpl element) {
        if (element.isEntity()) {
            modifiedEntities.add((EntityImpl) element);
        } else if (element.isRoleType()) {
            modifiedRoleTypes.add((RoleTypeImpl) element);
        } else if (element.isRelationType()) {
            modifiedRelationTypes.add((RelationTypeImpl) element);
        } else if (element.isRelation()){
            RelationImpl relation = (RelationImpl) element;
            modifiedRelations.add(relation);
            //Caching of relations in memory so they can be retrieved without needing a commit
            relationIndexCache.put(RelationImpl.generateNewHash(relation.type(), relation.allRolePlayers()), relation);
        } else if (element.isRule()){
            modifiedRules.add((RuleImpl) element);
        } else if (element.isResource()){
            modifiedResources.add((ResourceImpl) element);
        }
    }
    void trackForValidation(Casting casting) {
        modifiedCastings.add(casting);
    }

    /**
     *
     * @return All the relations which have been affected in the transaction
     */
    Map<String, RelationImpl> getRelationIndexCache(){
        return relationIndexCache;
    }

    /**
     *
     * @return All the types that have gained or lost instances and by how much
     */
    Map<ConceptId, Long> getShardingCount(){
        return shardingCount;
    }

    /**
     *
     * @return All the types currently cached in the transaction. Used for
     */
    Map<Label, TypeImpl> getTypeCache(){
        return typeCache;
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
    Map<ConceptId, ConceptImpl> getConceptCache() {
        return conceptCache;
    }

    /**
     *
     * @param concept The concept to nio longer track
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    void remove(ConceptImpl concept){
        modifiedEntities.remove(concept);
        modifiedRoleTypes.remove(concept);
        modifiedRelationTypes.remove(concept);
        modifiedRelations.remove(concept);
        modifiedRules.remove(concept);
        modifiedResources.remove(concept);

        conceptCache.remove(concept.getId());
        if (concept.isType()) {
            Label label = ((TypeImpl) concept).getLabel();
            typeCache.remove(label);
            labelCache.remove(label);
        }
    }

    /**
     * Gets a cached relation by index. This way we can find non committed relations quickly.
     *
     * @param index The current index of the relation
     */
    RelationImpl getCachedRelation(String index){
        return relationIndexCache.get(index);
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
            typeCache.put(type.getLabel(), type);
            labelCache.put(type.getLabel(), type.getLabelId());
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
    boolean isConceptCached(ConceptId id){
        return conceptCache.containsKey(id);
    }

    /**
     *
     * @param label The label of the type to cache
     * @return true if the concept is cached
     */
    boolean isTypeCached(Label label){
        return typeCache.containsKey(label);
    }

    /**
     *
     * @param label the type label which may be in the cache
     * @return true if the label is cached and has a valid mapping to a id
     */
    boolean isLabelCached(Label label){
        return labelCache.containsKey(label);
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
     * @param label The label of the type
     * @param <X> The type of the type
     * @return The cached type
     */
    <X extends Type> X getCachedType(Label label){
        //noinspection unchecked
        return (X) typeCache.get(label);
    }

    LabelId convertLabelToId(Label label){
        return labelCache.get(label);
    }

    void addedInstance(ConceptId conceptId){
        shardingCount.compute(conceptId, (key, value) -> value == null ? 1 : value + 1);
        cleanupShardingCount(conceptId);
    }
    void removedInstance(ConceptId conceptId){
        shardingCount.compute(conceptId, (key, value) -> value == null ? -1 : value - 1);
        cleanupShardingCount(conceptId);
    }
    private void cleanupShardingCount(ConceptId conceptId){
        if(shardingCount.get(conceptId) == 0) shardingCount.remove(conceptId);
    }

    Json getFormattedLog(){
        //Concepts In Need of Inspection
        Json conceptsForInspection = Json.object();
        conceptsForInspection.set(Schema.BaseType.RESOURCE.name(), loadConceptsForFixing(getModifiedResources()));

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
    private  <X extends ThingImpl> Json loadConceptsForFixing(Set<X> instances){
        Map<String, Set<String>> conceptByIndex = new HashMap<>();
        instances.forEach(concept ->
                conceptByIndex.computeIfAbsent(concept.getIndex(), (e) -> new HashSet<>()).add(concept.getId().getValue()));
        return Json.make(conceptByIndex);
    }

    //--------------------------------------- Concepts Needed For Validation -------------------------------------------
    Set<EntityImpl> getModifiedEntities() {
        return modifiedEntities;
    }

    Set<RoleTypeImpl> getModifiedRoleTypes() {
        return modifiedRoleTypes;
    }

    Set<RelationTypeImpl> getModifiedRelationTypes() {
        return modifiedRelationTypes;
    }
    Set<RelationImpl> getModifiedRelations() {
        return modifiedRelations;
    }

    Set<RuleImpl> getModifiedRules() {
        return modifiedRules;
    }

    Set<ResourceImpl> getModifiedResources() {
        return modifiedResources;
    }

    Set<Casting> getModifiedCastings() {
        return modifiedCastings;
    }

    //--------------------------------------- Transaction Specific Meta Data -------------------------------------------
    void closeTx(String closedReason){
        isTxOpen = false;
        this.closedReason = closedReason;
        modifiedEntities.clear();
        modifiedRoleTypes.clear();
        modifiedRelationTypes.clear();
        modifiedRelations.clear();
        modifiedRules.clear();
        modifiedResources.clear();
        modifiedCastings.clear();
        relationIndexCache.clear();
        shardingCount.clear();
        conceptCache.clear();
        typeCache.clear();
        labelCache.clear();
    }
    void openTx(GraknTxType txType){
        isTxOpen = true;
        this.txType = txType;
        closedReason = null;
    }
    boolean isTxOpen(){
        return isTxOpen;
    }

    void showImplicitTypes(boolean flag){
        showImplicitTypes = flag;
    }
    boolean implicitTypesVisible(){
        return showImplicitTypes;
    }

    GraknTxType txType(){
        return txType;
    }

    String getClosedReason(){
        return closedReason;
    }
}
