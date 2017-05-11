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
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.google.common.collect.ImmutableSet;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    private final Map<TypeLabel, TypeImpl> typeCache = new HashMap<>();
    private final Map<TypeLabel, Integer> labelCache = new HashMap<>();

    //We Track Modified Concepts For Validation
    private final Set<ConceptImpl> modifiedConcepts = new HashSet<>();

    //We Track Casting Explicitly For Post Processing
    private final Set<CastingImpl> modifiedCastings = new HashSet<>();

    //We Track Resource Explicitly for Post Processing
    private final Set<ResourceImpl> modifiedResources = new HashSet<>();

    //We Track Relations so that we can look them up before they are completely defined and indexed on commit
    private final Map<String, RelationImpl> modifiedRelations = new HashMap<>();

    //We Track the number of instances each type has lost or gained
    private final Map<TypeLabel, Long> instanceCount = new HashMap<>();

    //Transaction Specific Meta Data
    private boolean isTxOpen = false;
    private boolean showImplicitTypes = false;
    private boolean txReadOnly = false;
    private String closedReason = null;
    private Map<TypeLabel, Type> cloningCache = new HashMap<>();

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
        Map<TypeLabel, Type> cachedOntologySnapshot = graphCache.getCachedTypes();
        Map<TypeLabel, Integer> cachedLabelsSnapshot = graphCache.getCachedLabels();

        //Read central cache into txCache cloning only base concepts. Sets clones later
        for (Type type : cachedOntologySnapshot.values()) {
            cacheConcept((TypeImpl) cacheClone(type));
        }

        //Iterate through cached clones completing the cloning process.
        //This part has to be done in a separate iteration otherwise we will infinitely recurse trying to clone everything
        for (Type type : ImmutableSet.copyOf(cloningCache.values())) {
            //noinspection unchecked
            ((TypeImpl) type).copyCachedConcepts(cachedOntologySnapshot.get(type.getLabel()));
        }

        //Load Labels Separately. We do this because the TypeCache may have expired.
        cachedLabelsSnapshot.forEach(this::cacheLabel);

        //Purge clone cache to save memory
        cloningCache.clear();
    }

    /**
     *
     * @param type The type to clone
     * @param <X> The type of the concept
     * @return The newly deep cloned set
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    <X extends Type> X cacheClone(X type){
        //Is a cloning even needed?
        if(cloningCache.containsKey(type.getLabel())){
            return (X) cloningCache.get(type.getLabel());
        }

        //If the clone has not happened then make a new one
        Type clonedType = type.copy();

        //Update clone cache so we don't clone multiple concepts in the same transaction
        cloningCache.put(clonedType.getLabel(), clonedType);

        return (X) clonedType;
    }

    /**
     *
     * @param types a set of concepts to clone
     * @param <X> the type of those concepts
     * @return the set of concepts deep cloned
     */
    <X extends Type> Set<X> cacheClone(Set<X> types){
        return types.stream().map(this::cacheClone).collect(Collectors.toSet());
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
            modifiedRelations.put(RelationImpl.generateNewHash(relation.type(), relation.allRolePlayers()), relation);
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
     * @return All the types that have gained or lost instances and by how much
     */
    Map<TypeLabel, Long> getInstanceCount(){
        return instanceCount;
    }

    /**
     *
     * @return All the types currently cached in the transaction. Used for
     */
    Map<TypeLabel, TypeImpl> getTypeCache(){
        return typeCache;
    }

    /**
     *
     * @return All the types labels currently cached in the transaction.
     */
    Map<TypeLabel, Integer> getLabelCache(){
        return labelCache;
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
            TypeLabel label = ((TypeImpl) concept).getLabel();
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
            typeCache.put(type.getLabel(), type);
            if(!type.isShard()) labelCache.put(type.getLabel(), type.getTypeId());
        }
    }


    /**
     * Caches the mapping of a type label to a type id. This is necessary in order for ANY types to be looked up.
     *
     * @param label The type label to cache
     * @param id Its equivalent id which can be looked up quickly in the graph
     */
    private void cacheLabel(TypeLabel label, Integer id){
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
    boolean isTypeCached(TypeLabel label){
        return typeCache.containsKey(label);
    }

    /**
     *
     * @param label the type label which may be in the cache
     * @return true if the label is cached and has a valid mapping to a id
     */
    boolean isLabelCached(TypeLabel label){
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
    <X extends Type> X getCachedType(TypeLabel label){
        //noinspection unchecked
        return (X) typeCache.get(label);
    }

    Integer convertLabelToId(TypeLabel label){
        return labelCache.get(label);
    }

    void addedInstance(TypeLabel name){
        instanceCount.compute(name, (key, value) -> value == null ? 1 : value + 1);
        cleanupInstanceCount(name);
    }
    void removedInstance(TypeLabel name){
        instanceCount.compute(name, (key, value) -> value == null ? -1 : value - 1);
        cleanupInstanceCount(name);
    }
    private void cleanupInstanceCount(TypeLabel name){
        if(instanceCount.get(name) == 0) instanceCount.remove(name);
    }

    JSONObject getFormattedLog(){
        //Concepts In Need of Inspection
        JSONObject conceptsForInspection = new JSONObject();
        conceptsForInspection.put(Schema.BaseType.CASTING.name(), loadConceptsForFixing(getModifiedCastings()));
        conceptsForInspection.put(Schema.BaseType.RESOURCE.name(), loadConceptsForFixing(getModifiedResources()));

        //Types with instance changes
        JSONArray typesWithInstanceChanges = new JSONArray();

        getInstanceCount().forEach((key, value) -> {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(REST.Request.COMMIT_LOG_TYPE_NAME, key.getValue());
            jsonObject.put(REST.Request.COMMIT_LOG_INSTANCE_COUNT, value);
            typesWithInstanceChanges.put(jsonObject);
        });

        //Final Commit Log
        JSONObject formattedLog = new JSONObject();
        formattedLog.put(REST.Request.COMMIT_LOG_FIXING, conceptsForInspection);
        formattedLog.put(REST.Request.COMMIT_LOG_COUNTING, typesWithInstanceChanges);

        return formattedLog;
    }
    private  <X extends InstanceImpl> JSONObject loadConceptsForFixing(Set<X> instances){
        Map<String, Set<String>> conceptByIndex = new HashMap<>();
        instances.forEach(concept ->
                conceptByIndex.computeIfAbsent(concept.getIndex(), (e) -> new HashSet<>()).add(concept.getId().getValue()));
        return new JSONObject(conceptByIndex);
    }

    //--------------------------------------- Transaction Specific Meta Data -------------------------------------------
    void closeTx(String closedReason){
        isTxOpen = false;
        this.closedReason = closedReason;
        modifiedConcepts.clear();
        modifiedCastings.clear();
        modifiedResources.clear();
        conceptCache.clear();
        typeCache.clear();
        labelCache.clear();
    }
    void openTx(GraknTxType txType){
        isTxOpen = true;
        txReadOnly = GraknTxType.READ.equals(txType);
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

    boolean isTxReadOnly(){
        return txReadOnly;
    }

    String getClosedReason(){
        return closedReason;
    }
}
