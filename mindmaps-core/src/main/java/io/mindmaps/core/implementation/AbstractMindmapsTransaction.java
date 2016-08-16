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
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.MindmapsTransaction;
import io.mindmaps.core.model.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;

public abstract class AbstractMindmapsTransaction implements MindmapsTransaction, AutoCloseable {
    protected final Logger LOG = LoggerFactory.getLogger(AbstractMindmapsTransaction.class);
    private final ElementFactory elementFactory;
    private final ConceptLog conceptLog;
    private final boolean batchLoadingEnabled;
    private Graph transaction;

    public AbstractMindmapsTransaction(Graph transaction, boolean batchLoadingEnabled) {
        this.transaction = transaction;
        this.batchLoadingEnabled = batchLoadingEnabled;
        conceptLog = new ConceptLog();
        elementFactory = new ElementFactory(this);
    }

    public abstract AbstractMindmapsGraph getRootGraph();

    public boolean isBatchLoadingEnabled(){
        return batchLoadingEnabled;
    }

    @SuppressWarnings("unchecked")
    public void initialiseMetaConcepts(){
        if(isMetaOntologyNotInitialised()){
            LOG.info("Initialising new transaction . . .");

            TypeImpl type = elementFactory.buildConceptType(addVertex(DataType.BaseType.TYPE));
            TypeImpl entityType = elementFactory.buildConceptType(addVertex(DataType.BaseType.TYPE));
            TypeImpl relationType = elementFactory.buildConceptType(addVertex(DataType.BaseType.TYPE));
            TypeImpl resourceType = elementFactory.buildConceptType(addVertex(DataType.BaseType.TYPE));
            TypeImpl roleType = elementFactory.buildConceptType(addVertex(DataType.BaseType.TYPE));
            TypeImpl ruleType = elementFactory.buildConceptType(addVertex(DataType.BaseType.TYPE));
            RuleTypeImpl inferenceRuleType = elementFactory.buildRuleType(addVertex(DataType.BaseType.RULE_TYPE));
            RuleTypeImpl constraintRuleType = elementFactory.buildRuleType(addVertex(DataType.BaseType.RULE_TYPE));

            type.setProperty(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, DataType.ConceptMeta.TYPE.getId());
            entityType.setProperty(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, DataType.ConceptMeta.ENTITY_TYPE.getId());
            relationType.setProperty(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, DataType.ConceptMeta.RELATION_TYPE.getId());
            resourceType.setProperty(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, DataType.ConceptMeta.RESOURCE_TYPE.getId());
            roleType.setProperty(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, DataType.ConceptMeta.ROLE_TYPE.getId());
            ruleType.setProperty(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, DataType.ConceptMeta.RULE_TYPE.getId());
            inferenceRuleType.setProperty(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, DataType.ConceptMeta.INFERENCE_RULE.getId());
            constraintRuleType.setProperty(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, DataType.ConceptMeta.CONSTRAINT_RULE.getId());

            type.setType(type.getId());
            relationType.setType(type.getId());
            roleType.setType(type.getId());
            resourceType.setType(type.getId());
            ruleType.setType(type.getId());
            entityType.setType(type.getId());

            type.type(type);

            relationType.superType(type);
            roleType.superType(type);
            resourceType.superType(type);
            ruleType.superType(type);
            entityType.superType(type);

            inferenceRuleType.type(ruleType);
            constraintRuleType.type(ruleType);
        }
    }

    public boolean isMetaOntologyNotInitialised(){
        return getMetaType() == null;
    }

    Graph getTinkerPopGraph(){
        if(transaction == null){
            throw new GraphRuntimeException(ErrorMessage.CLOSED.getMessage(this.getClass().getName()));
        }
        return transaction;
    }

    public GraphTraversalSource getTinkerTraversal(){
        ReadOnlyStrategy readOnlyStrategy = ReadOnlyStrategy.instance();
        return getTinkerPopGraph().traversal().asBuilder().with(readOnlyStrategy).create(getTinkerPopGraph());
    }

    protected void setTinkerPopGraph(Graph graph){
        this.transaction = graph;
    }

    public ElementFactory getElementFactory(){
        return elementFactory;
    }

    //----------------------------------------------General Functionality-----------------------------------------------
    private EdgeImpl addEdge(Concept from, Concept to, DataType.EdgeLabel type){
        return ((ConceptImpl)from).addEdge((ConceptImpl) to, type);
    }

    public ConceptImpl getConcept(DataType.ConceptPropertyUnique key, String value) {
        Iterator<Vertex> vertices = getTinkerTraversal().V().has(key.name(), value);

        if(vertices.hasNext()){
            Vertex vertex = vertices.next();
            if(vertices.hasNext())
                throw new MoreThanOneConceptException(ErrorMessage.TOO_MANY_CONCEPTS.getMessage(key.name(), value));
            return elementFactory.buildUnknownConcept(vertex);
        } else {
            return null;
        }
    }


    public Set<ConceptImpl> getModifiedConcepts(){
        return conceptLog.getModifiedConcepts();
    }

    public Set<String> getModifiedCastingIds(){
        Set<String> relationIds = new HashSet<>();
        conceptLog.getModifiedCastings().forEach(c -> relationIds.add(c.getId()));
        return relationIds;
    }

    public ConceptLog getConceptLog() {
        return conceptLog;
    }

    //----------------------------------------------Concept Functionality-----------------------------------------------
    //------------------------------------ Construction
    private Vertex addVertex(DataType.BaseType baseType){
        return getTinkerPopGraph().addVertex(baseType.name());
    }
    private Vertex addInstanceVertex(DataType.BaseType baseType, Type type){
        Vertex v = addVertex(baseType);
        v.property(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), generateInstanceId(baseType, type));
        return v;
    }
    private String generateInstanceId(DataType.BaseType baseType, Type type){
        return baseType.name() + "-" + type.getId() + "-" + UUID.randomUUID().toString();
    }

    private Vertex putVertex(String itemIdentifier, DataType.BaseType baseType){
        if(DataType.ConceptMeta.isMetaId(itemIdentifier)){
            throw new ConceptException(ErrorMessage.ID_RESERVED.getMessage(itemIdentifier));
        }

        Vertex vertex;
        ConceptImpl concept = getConcept(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, itemIdentifier);
        if(concept == null) {
            vertex = addVertex(baseType);
            vertex.property(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), itemIdentifier);
        } else {
            if(!baseType.name().equals(concept.getBaseType()))
                throw new ConceptIdNotUniqueException(concept, itemIdentifier);
            vertex = concept.getVertex();
        }
        return vertex;
    }

    @Override
    public Entity putEntity(String itemIdentifier, EntityType type) {
        EntityImpl thing = elementFactory.buildEntity(putVertex(itemIdentifier, DataType.BaseType.ENTITY));
        thing.type(type);
        return thing;
    }

    @Override
    public Entity addEntity(EntityType type) {
        return elementFactory.buildEntity(addInstanceVertex(DataType.BaseType.ENTITY, type)).type(type);
    }

    @Override
    public EntityType putEntityType(String itemIdentifier) {
        return elementFactory.buildEntityType(putConceptType(itemIdentifier, DataType.BaseType.ENTITY_TYPE, getMetaEntityType()));
    }
    private Type putConceptType(String itemIdentifier, DataType.BaseType baseType, Type metaType) {
        TypeImpl conceptType = elementFactory.buildSpecificConceptType(putVertex(itemIdentifier, baseType));
        conceptType.setId(itemIdentifier);
        conceptType.type(metaType);
        return conceptType;
    }

    @Override
    public RelationType putRelationType(String itemIdentifier) {
        return elementFactory.buildRelationType(putConceptType(itemIdentifier, DataType.BaseType.RELATION_TYPE, getMetaRelationType()));
    }

    @Override
    public RoleType putRoleType(String itemIdentifier) {
        return elementFactory.buildRoleType(putConceptType(itemIdentifier, DataType.BaseType.ROLE_TYPE, getMetaRoleType()));
    }

    @Override
    public <V> ResourceType <V> putResourceType(String id, Data<V> type) {
        return elementFactory.buildResourceType(putConceptType(id, DataType.BaseType.RESOURCE_TYPE, getMetaResourceType()), type);
    }

    @Override
    public <V> Resource<V> putResource(String itemIdentifier, ResourceType<V> type) {
        ResourceImpl<V> resource = elementFactory.buildResource(putVertex(itemIdentifier, DataType.BaseType.RESOURCE));
        resource.type(type);
        return resource;
    }

    @Override
    public <V> Resource<V> addResource(ResourceType<V> type) {
        ResourceImpl<V> resource = elementFactory.buildResource(addInstanceVertex(DataType.BaseType.RESOURCE, type));
        resource.type(type);
        return resource;
    }

    @Override
    public RuleType putRuleType(String itemIdentifier) {
        return elementFactory.buildRuleType(putConceptType(itemIdentifier, DataType.BaseType.RULE_TYPE, getMetaRuleType()));
    }

    @Override
    public Rule putRule(String itemIdentifier, RuleType type) {
        return elementFactory.buildRule(putVertex(itemIdentifier, DataType.BaseType.RULE)).type(type);
    }

    @Override
    public Rule addRule(RuleType type) {
        return elementFactory.buildRule(addInstanceVertex(DataType.BaseType.RULE, type)).type(type);
    }

    @Override
    public Relation putRelation(String itemIdentifier, RelationType type) {
        RelationImpl relation = elementFactory.buildRelation(putVertex(itemIdentifier, DataType.BaseType.RELATION));
        relation.setHash(null);
        relation.type(type);
        return relation;
    }

    @Override
    public Relation putRelation(RelationType type, Map<RoleType, Instance> roleMap) {
        Relation relation = getRelation(type, roleMap);
        if(relation == null){
            relation = addRelation(type);
            for (Map.Entry<RoleType, Instance> entry : roleMap.entrySet()) {
                relation.putRolePlayer(entry.getKey(), entry.getValue());
            }
        }
        return relation;
    }

    @Override
    public Relation addRelation(RelationType type) {
        return elementFactory.buildRelation(addInstanceVertex(DataType.BaseType.RELATION, type)).type(type);
    }

    //------------------------------------ Lookup
    @SuppressWarnings("unchecked")
    private <T extends Concept> T validConceptOfType(Concept concept, Class type){
        if(concept != null &&  type.isInstance(concept)){
            return (T) concept;
        }
        return null;
    }
    public ConceptImpl getConceptByBaseIdentifier(long baseIdentifier) {
        GraphTraversal<Vertex, Vertex> traversal = getTinkerTraversal().V(baseIdentifier);
        if (traversal.hasNext()) {
            return elementFactory.buildUnknownConcept(traversal.next());
        } else {
            return null;
        }
    }
    @Override
    public Concept getConcept(String id) {
        return getConcept(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, id);
    }
    @Override
    public Concept getConceptBySubject(String subject) {
        return getConcept(DataType.ConceptPropertyUnique.SUBJECT_IDENTIFIER, subject);
    }
    @Override
    public <V> Collection<Concept> getConceptsByValue(V value) {
        return getConceptsByValue(value, ConceptImpl.class, Data.SUPPORTED_TYPES.get(value.getClass().getTypeName()));
    }

    @Override
    public Type getType(String id) {
        return validConceptOfType(getConcept(id), TypeImpl.class);
    }

    @Override
    public Type getTypeBySubject(String subject) {
        return validConceptOfType(getConceptBySubject(subject), TypeImpl.class);
    }

    @Override
    public Collection<Type> getTypesByValue(String value) {
        return getConceptsByValue(value, TypeImpl.class, Data.STRING);
    }

    @Override
    public Instance getInstance(String id) {
        return validConceptOfType(getConcept(id), InstanceImpl.class);
    }

    @Override
    public Instance getInstanceBySubject(String subject) {
        return validConceptOfType(getConceptBySubject(subject), InstanceImpl.class);
    }

    @Override
    public <V> Collection<Instance> getInstancesByValue(V value) {
        return getConceptsByValue(value, InstanceImpl.class, Data.STRING);
    }

    private <T extends Concept> HashSet<T> getConceptsByValue(Object value, Class type, Data dataType){
        HashSet<T> concepts = new HashSet<>();

        getTinkerTraversal().V().has(dataType.getConceptProperty().name(), value).
                forEachRemaining(v -> {
                    T concept = validConceptOfType(elementFactory.buildUnknownConcept(v), type);
                    if (concept != null)
                        concepts.add(concept);
                });

        return concepts;
    }

    @Override
    public Entity getEntity(String id) {
        return validConceptOfType(getConcept(id), EntityImpl.class);
    }
    @Override
    public Entity getEntityBySubject(String subject) {
        return validConceptOfType(getConceptBySubject(subject), EntityImpl.class);
    }

    @Override
    public Collection<Entity> getEntitiesByValue(String value) {
       return getConceptsByValue(value, EntityImpl.class, Data.STRING);
    }

    @Override
    public <V> Resource<V> getResource(String id) {
        return validConceptOfType(getConcept(id), ResourceImpl.class);
    }

    @Override
    public <V> Resource<V> getResourceBySubject(String subject) {
        return validConceptOfType(getConceptBySubject(subject), ResourceImpl.class);
    }

    @Override
    public <V> Collection<Resource<V>> getResourcesByValue(V value) {
        return getConceptsByValue(value, ResourceImpl.class, Data.SUPPORTED_TYPES.get(value.getClass().getTypeName()));
    }

    @Override
    public Rule getRule(String id) {
        return validConceptOfType(getConcept(id), RuleImpl.class);
    }

    @Override
    public Rule getRuleBySubject(String subject) {
        return validConceptOfType(getConceptBySubject(subject), RuleImpl.class);
    }

    @Override
    public Collection<Rule> getRulesByValue(String value) {
        return getConceptsByValue(value, RuleImpl.class, Data.STRING);
    }

    @Override
    public EntityType getEntityType(String id) {
        return validConceptOfType(getConcept(id), EntityTypeImpl.class);
    }

    @Override
    public EntityType getEntityTypeBySubject(String subject) {
        return validConceptOfType(getConceptBySubject(subject), EntityTypeImpl.class);
    }

    @Override
    public Collection<EntityType> getEntityTypesByValue(String value) {
        return getConceptsByValue(value, EntityTypeImpl.class, Data.STRING);
    }

    @Override
    public RelationType getRelationType(String id) {
        return validConceptOfType(getConcept(id), RelationTypeImpl.class);
    }

    @Override
    public RelationType getRelationTypeBySubject(String subject) {
        return validConceptOfType(getConceptBySubject(subject), RelationTypeImpl.class);
    }

    @Override
    public Collection<RelationType> getRelationTypesByValue(String value) {
        return getConceptsByValue(value, RelationTypeImpl.class, Data.STRING);
    }

    @Override
    public <V> ResourceType<V> getResourceType(String id) {
        return validConceptOfType(getConcept(id), ResourceTypeImpl.class);
    }

    @Override
    public <V> ResourceType<V> getResourceTypeBySubject(String subject) {
        return validConceptOfType(getConceptBySubject(subject), ResourceTypeImpl.class);
    }

    @Override
    public <V> Collection<ResourceType<V>> getResourceTypesByValue(String value) {
        return getConceptsByValue(value, ResourceTypeImpl.class, Data.STRING);
    }

    @Override
    public RoleType getRoleType(String id) {
        return validConceptOfType(getConcept(id), RoleTypeImpl.class);
    }

    @Override
    public RoleType getRoleTypeBySubject(String subject) {
        return validConceptOfType(getConceptBySubject(subject), RoleTypeImpl.class);
    }

    @Override
    public Collection<RoleType> getRoleTypesByValue(String value) {
        return getConceptsByValue(value, RoleTypeImpl.class, Data.STRING);
    }

    @Override
    public RuleType getRuleType(String id) {
        return validConceptOfType(getConcept(id), RuleTypeImpl.class);
    }

    @Override
    public RuleType getRuleTypeBySubject(String subject) {
        return validConceptOfType(getConceptBySubject(subject), RuleTypeImpl.class);
    }

    @Override
    public Collection<RuleType> getRuleTypesByValue(String value) {
        return getConceptsByValue(value, RuleTypeImpl.class, Data.STRING);
    }

    private Type getConceptType(String id){
        return validConceptOfType(getConcept(id), TypeImpl.class);
    }

    @Override
    public Type getMetaType() {
        return getConceptType(DataType.ConceptMeta.TYPE.getId());
    }

    @Override
    public Type getMetaRelationType() {
        return getConceptType(DataType.ConceptMeta.RELATION_TYPE.getId());
    }

    @Override
    public Type getMetaRoleType() {
        return getConceptType(DataType.ConceptMeta.ROLE_TYPE.getId());
    }

    @Override
    public Type getMetaResourceType() {
        return getConceptType(DataType.ConceptMeta.RESOURCE_TYPE.getId());
    }

    @Override
    public Type getMetaEntityType() {
        return getConceptType(DataType.ConceptMeta.ENTITY_TYPE.getId());
    }

    @Override
    public Type getMetaRuleType(){
        return getConceptType(DataType.ConceptMeta.RULE_TYPE.getId());
    }

    @Override
    public RuleType getMetaRuleInference() {
        return getConceptType(DataType.ConceptMeta.INFERENCE_RULE.getId()).asRuleType();
    }

    @Override
    public RuleType getMetaRuleConstraint() {
        return getConceptType(DataType.ConceptMeta.CONSTRAINT_RULE.getId()).asRuleType();
    }

    //-----------------------------------------------Casting Functionality----------------------------------------------
    //------------------------------------ Construction
    private CastingImpl addCasting(RoleTypeImpl role, InstanceImpl rolePlayer){
        CastingImpl casting = elementFactory.buildCasting(addInstanceVertex(DataType.BaseType.CASTING, role)).setHash(role, rolePlayer);
        casting.type(role);
        if(rolePlayer != null) {
            EdgeImpl castingToRolePlayer = addEdge(casting, rolePlayer, DataType.EdgeLabel.ROLE_PLAYER); // Casting to RolePlayer
            castingToRolePlayer.setProperty(DataType.EdgeProperty.ROLE_TYPE, role.getId());
        }
        return casting;
    }
    public CastingImpl putCasting(RoleTypeImpl role, InstanceImpl rolePlayer, RelationImpl relation){
        CastingImpl foundCasting  = null;
        if(rolePlayer != null)
            foundCasting = getCasting(role, rolePlayer);

        if(foundCasting == null){
            foundCasting = addCasting(role, rolePlayer);
        }

        EdgeImpl assertionToCasting = addEdge(relation, foundCasting, DataType.EdgeLabel.CASTING);// Relation To Casting
        assertionToCasting.setProperty(DataType.EdgeProperty.ROLE_TYPE, role.getId());

        putShortcutEdges(relation, relation.type());

        return foundCasting;
    }

    //------------------------------------ Lookup
    private CastingImpl getCasting(RoleTypeImpl role, InstanceImpl rolePlayer){
        try {
            String hash = CastingImpl.generateNewHash(role, rolePlayer);
            ConceptImpl concept = getConcept(DataType.ConceptPropertyUnique.INDEX, hash);
            if (concept != null)
                return concept.asCasting();
            else
                return null;
        } catch(GraphRuntimeException e){
            throw new MoreThanOneConceptException(ErrorMessage.TOO_MANY_CASTINGS.getMessage(role, rolePlayer));
        }
    }

    public void putShortcutEdges(Relation relation, RelationType relationType){
        Map<RoleType, Instance> roleMap = relation.rolePlayers();
        if(roleMap.size() > 1) {
            for(Map.Entry<RoleType, Instance> from : roleMap.entrySet()){
                for(Map.Entry<RoleType, Instance> to :roleMap.entrySet()){
                    if(from.getValue() != null && to.getValue() != null){
                        if(from.getKey() != to.getKey())
                            putShortcutEdge(
                                    elementFactory.buildRelation(relation),
                                    elementFactory.buildRelationType(relationType),
                                    elementFactory.buildRoleType(from.getKey()),
                                    elementFactory.buildSpecificInstance(from.getValue()),
                                    elementFactory.buildRoleType(to.getKey()),
                                    elementFactory.buildSpecificInstance(to.getValue()));
                    }
                }
            }
        }
        ((RelationImpl)relation).setHash(relation.rolePlayers());
    }

    private void putShortcutEdge(RelationImpl  relation, RelationTypeImpl  relationType, RoleTypeImpl  fromRole, InstanceImpl fromRolePlayer, RoleTypeImpl  toRole, InstanceImpl toRolePlayer){
        String hash = calculateShortcutHash(relation, relationType, fromRole, fromRolePlayer, toRole, toRolePlayer);
        boolean exists = getTinkerTraversal().V(fromRolePlayer.getBaseIdentifier()).
                    local(outE(DataType.EdgeLabel.SHORTCUT.getLabel()).has(DataType.EdgeProperty.SHORTCUT_HASH.name(), hash)).
                    hasNext();

        if (!exists) {
            EdgeImpl edge = addEdge(fromRolePlayer, toRolePlayer, DataType.EdgeLabel.SHORTCUT);
            edge.setProperty(DataType.EdgeProperty.RELATION_TYPE_ID, relationType.getId());
            edge.setProperty(DataType.EdgeProperty.RELATION_ID, relation.getId());

            if (fromRolePlayer.getId() != null)
                edge.setProperty(DataType.EdgeProperty.FROM_ID, fromRolePlayer.getId());
            edge.setProperty(DataType.EdgeProperty.FROM_ROLE, fromRole.getId());

            if (toRolePlayer.getId() != null)
                edge.setProperty(DataType.EdgeProperty.TO_ID, toRolePlayer.getId());
            edge.setProperty(DataType.EdgeProperty.TO_ROLE, toRole.getId());

            edge.setProperty(DataType.EdgeProperty.FROM_TYPE, fromRolePlayer.getParentIsa().getId());
            edge.setProperty(DataType.EdgeProperty.TO_TYPE, toRolePlayer.getParentIsa().getId());
            edge.setProperty(DataType.EdgeProperty.SHORTCUT_HASH, hash);
        }
    }

    private String calculateShortcutHash(RelationImpl relation, RelationTypeImpl relationType, RoleTypeImpl fromRole, InstanceImpl fromRolePlayer, RoleTypeImpl toRole, InstanceImpl toRolePlayer){
        String hash = "";
        String relationIdValue = relationType.getId();
        String fromIdValue = fromRolePlayer.getId();
        String fromRoleValue = fromRole.getId();
        String toIdValue = toRolePlayer.getId();
        String toRoleValue = toRole.getId();
        Long assertionIdValue = relation.getBaseIdentifier();

        if(relationIdValue != null)
            hash += relationIdValue;
        if(fromIdValue != null)
            hash += fromIdValue;
        if(fromRoleValue != null)
            hash += fromRoleValue;
        if(toIdValue != null)
            hash += toIdValue;
        if(toRoleValue != null)
            hash += toRoleValue;
        hash += String.valueOf(assertionIdValue);

        return hash;
    }

    //------------------------------------ getRelation
    @Override
    public Relation getRelation(RelationType relationType, Map<RoleType, Instance> roleMap){
        String hash = RelationImpl.generateNewHash(relationType, roleMap);
        Concept concept = getConcept(DataType.ConceptPropertyUnique.INDEX, hash);
        if(concept == null)
            return null;
        return concept.asRelation();
    }

    @Override
    public Relation getRelation(String id) {
        ConceptImpl concept = getConcept(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, id);
        if(concept != null && DataType.BaseType.RELATION.name().equals(concept.getBaseType()))
            return elementFactory.buildRelation(concept);
        else
            return null;
    }

    @Override
    public void refresh() throws Exception {
        close();
        getConceptLog().clearTransaction();
        setTinkerPopGraph(getNewTransaction());
    }

    protected abstract Graph getNewTransaction();

    /**
     * Closes the current transaction rendering it unusable.
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        getConceptLog().clearTransaction();
        setTinkerPopGraph(null);
    }

    /**
     * Commits the graph
     * @throws MindmapsValidationException when the grpah does not conform to the object model
     */
    @Override
    public void commit() throws MindmapsValidationException {
        validateGraph();

        Map<DataType.BaseType, Set<String>> modifiedConcepts = new HashMap<>();
        Set<String> castings = getModifiedCastingIds();

        if(castings.size() > 0)
            modifiedConcepts.put(DataType.BaseType.CASTING, castings);

        LOG.info("Graph is valid. Committing graph . . . ");
        persistGraph();

        try {
            refresh();
        } catch (Exception e) {
            LOG.error("Failed to create new transaction after committing", e);
            e.printStackTrace();
        }
        LOG.info("Graph committed.");

        if(modifiedConcepts.size() > 0)
            submitCommitLogs(modifiedConcepts);
    }

    protected void persistGraph(){
        getTinkerPopGraph().tx().commit();
    }

    protected void validateGraph() throws MindmapsValidationException {
        LOG.info("Validating transaction . . . ");
        Validator validator = new Validator(this);
        if (!validator.validate()) {
            List<String> errors = validator.getErrorsFound();
            String error = ErrorMessage.VALIDATION.getMessage(errors.size());
            for (String s : errors) {
                error += s;
            }
            throw new MindmapsValidationException(error);
        }
    }

    protected void submitCommitLogs(Map<DataType.BaseType, Set<String>> concepts){
        LOG.info("Submitting commit logs to [" + getRootGraph().getCommitLogEndPoint() + "]");

        JSONArray jsonArray = new JSONArray();
        for (Map.Entry<DataType.BaseType, Set<String>> entry : concepts.entrySet()) {
            DataType.BaseType type = entry.getKey();

            for (String conceptId : entry.getValue()) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("id", conceptId);
                jsonObject.put("type", type.name());
                jsonArray.put(jsonObject);
            }

        }

        JSONObject postObject = new JSONObject();
        postObject.put("concepts", jsonArray);

        try {
            String result = EngineCommunicator.contactEngine(getRootGraph().getCommitLogEndPoint(), "POST", postObject.toString());
            LOG.info("Response from engine [" + result + "]");
        } catch (IllegalArgumentException e) {
            LOG.error(ErrorMessage.COULD_NOT_REACH_ENGINE.getMessage(getRootGraph().getCommitLogEndPoint()), e);
        }
    }

    //------------------------------------------ Fixing Code for Postprocessing ----------------------------------------
    /**
     * Merges duplicate castings if one is found.
     * @param castingId The id of the casting to check for duplicates
     * @return true if some castings were merged
     */
    public boolean fixDuplicateCasting(String castingId){
        //Get the Casting
        ConceptImpl concept = (ConceptImpl) getConcept(castingId);
        if(concept == null || !concept.isCasting())
            return false;

        //Check if the casting has duplicates
        CastingImpl casting = concept.asCasting();
        InstanceImpl rolePlayer = casting.getRolePlayer();
        RoleType role = casting.getRole();

        //Traversal here is used to take advantage of vertex centric index
        List<Vertex> castingVertices = getTinkerTraversal().V(rolePlayer.getBaseIdentifier()).
                inE(DataType.EdgeLabel.ROLE_PLAYER.getLabel()).
                has(DataType.EdgeProperty.ROLE_TYPE.name(), role.getId()).otherV().toList();

        Set<CastingImpl> castings = castingVertices.stream().map(elementFactory::buildCasting).collect(Collectors.toSet());

        if(castings.size() < 2){
            return false;
        }

        //Fix the duplicates
        castings.remove(casting);
        Set<RelationImpl> duplicateRelations = mergeCastings(casting, castings);

        //Remove Redundant Relations
        deleteDuplicateRelations(duplicateRelations);

        return true;
    }

    private void deleteDuplicateRelations(Set<RelationImpl> relations){
        for (RelationImpl relation : relations) {
            String relationID = relation.getId();

            //Kill Shortcut Edges
            relation.rolePlayers().values().forEach(instance -> {
                if(instance != null) {
                    List<Edge> edges = getTinkerTraversal().V().
                            has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), instance.getId()).
                            bothE(DataType.EdgeLabel.SHORTCUT.getLabel()).
                            has(DataType.EdgeProperty.RELATION_ID.name(), relationID).toList();

                    edges.forEach(Element::remove);
                }
            });

            relation.deleteNode();
        }
    }

    private Set<RelationImpl> mergeCastings(CastingImpl mainCasting, Set<CastingImpl> castings){
        RoleType role = mainCasting.getRole();
        Set<RelationImpl> relations = mainCasting.getRelations();
        Set<RelationImpl> relationsToClean = new HashSet<>();

        for (CastingImpl otherCasting : castings) {
            //Transfer assertion edges
            for(RelationImpl otherRelation : otherCasting.getRelations()){
                boolean transferEdge = true;

                //Check if an equivalent Relation is already connected to this casting. This could be a slow process
                for(Relation originalRelation: relations){
                    if(relationsEqual(originalRelation, otherRelation)){
                        relationsToClean.add(otherRelation);
                        transferEdge = false;
                        break;
                    }
                }

                //Perform the transfer
                if(transferEdge) {
                    EdgeImpl assertionToCasting = addEdge(otherRelation, mainCasting, DataType.EdgeLabel.CASTING);
                    assertionToCasting.setProperty(DataType.EdgeProperty.ROLE_TYPE, role.getId());
                }
            }

            getTinkerTraversal().V(otherCasting.getBaseIdentifier()).next().remove();
        }

        return relationsToClean;
    }

    private boolean relationsEqual(Relation mainRelation, Relation otherRelation){
        return mainRelation.rolePlayers().equals(otherRelation.rolePlayers()) &&
                mainRelation.type().equals(otherRelation.type());
    }

}
