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

import io.mindmaps.MindmapsGraph;
import io.mindmaps.constants.DataType;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.constants.RESTUtil;
import io.mindmaps.core.Data;
import io.mindmaps.core.implementation.exception.ConceptException;
import io.mindmaps.core.implementation.exception.ConceptIdNotUniqueException;
import io.mindmaps.core.implementation.exception.GraphRuntimeException;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.implementation.exception.MoreThanOneConceptException;
import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Entity;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.Instance;
import io.mindmaps.core.model.Relation;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.Resource;
import io.mindmaps.core.model.ResourceType;
import io.mindmaps.core.model.RoleType;
import io.mindmaps.core.model.Rule;
import io.mindmaps.core.model.RuleType;
import io.mindmaps.core.model.Type;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;

public abstract class AbstractMindmapsGraph<G extends Graph> implements MindmapsGraph {
    private final Logger LOG = LoggerFactory.getLogger(AbstractMindmapsGraph.class);
    private final ThreadLocal<ConceptLog> context = new ThreadLocal<>();
    private final ElementFactory elementFactory;
    //private final ConceptLog conceptLog;
    private final String keyspace;
    private final String engine;
    private final boolean batchLoadingEnabled;
    private G graph;

    public AbstractMindmapsGraph(G graph, String keyspace, String engine, boolean batchLoadingEnabled) {
        this.graph = graph;
        this.keyspace = keyspace;
        this.engine = engine;
        this.batchLoadingEnabled = batchLoadingEnabled;
        elementFactory = new ElementFactory(this);

        if(initialiseMetaConcepts()) {
            try {
                commit();
            } catch (MindmapsValidationException e) {
                throw new RuntimeException(ErrorMessage.CREATING_ONTOLOGY_ERROR.getMessage(e.getMessage()));
            }
        }
    }

    @Override
    public String getKeyspace(){
        return keyspace;
    }

    public boolean isBatchLoadingEnabled(){
        return batchLoadingEnabled;
    }

    @SuppressWarnings("unchecked")
    public boolean initialiseMetaConcepts(){
        if(isMetaOntologyNotInitialised()){
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

            return true;
        }

        return false;
    }

    public boolean isMetaOntologyNotInitialised(){
        return getMetaType() == null;
    }

    public G getTinkerPopGraph(){
        if(graph == null){
            throw new GraphRuntimeException(ErrorMessage.CLOSED.getMessage(this.getClass().getName()));
        }
        return graph;
    }

    @Override
    public GraphTraversalSource getTinkerTraversal(){
        ReadOnlyStrategy readOnlyStrategy = ReadOnlyStrategy.instance();
        return getTinkerPopGraph().traversal().asBuilder().with(readOnlyStrategy).create(getTinkerPopGraph());
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
        return getConceptLog().getModifiedConcepts();
    }

    public Set<String> getModifiedCastingIds(){
        Set<String> relationIds = new HashSet<>();
        getConceptLog().getModifiedCastings().forEach(c -> relationIds.add(c.getId()));
        return relationIds;
    }

    public ConceptLog getConceptLog() {
        ConceptLog conceptLog = context.get();
        if(conceptLog == null){
            context.set(conceptLog = new ConceptLog());
        }
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
    public <V> Resource<V> putResource(V value, ResourceType<V> type) {
        ResourceImpl<V> resource;
        String index = ResourceImpl.generateResourceIndex(type.getId(), value.toString());
        ConceptImpl concept = getConcept(DataType.ConceptPropertyUnique.INDEX, index);

        if(concept == null){
            resource = elementFactory.buildResource(addInstanceVertex(DataType.BaseType.RESOURCE, type));
            resource.type(type);
            resource.setValue(value);
        } else {
            if(concept.isResource()) {
                resource = (ResourceImpl<V>) concept.asResource();
            } else {
                throw new ConceptException(ErrorMessage.RESOURCE_INDEX_ALREADY_TAKEN.getMessage(index, concept));
            }
        }
        return resource;
    }


    @Override
    public RuleType putRuleType(String itemIdentifier) {
        return elementFactory.buildRuleType(putConceptType(itemIdentifier, DataType.BaseType.RULE_TYPE, getMetaRuleType()));
    }

    @Override
    public Rule putRule(String itemIdentifier, String lhs, String rhs, RuleType type) {
        return elementFactory.buildRule(putVertex(itemIdentifier, DataType.BaseType.RULE), lhs, rhs).type(type);
    }

    @Override
    public Rule addRule(String lhs, String rhs, RuleType type) {
        return elementFactory.buildRule(addInstanceVertex(DataType.BaseType.RULE, type), lhs, rhs).type(type);
    }

    @Override
    public Relation putRelation(String itemIdentifier, RelationType type) {
        RelationImpl relation = elementFactory.buildRelation(putVertex(itemIdentifier, DataType.BaseType.RELATION));
        relation.setHash(null);
        relation.type(type);
        return relation;
    }

    @Override
    public Relation addRelation(RelationType type) {
        RelationImpl relation = elementFactory.buildRelation(addInstanceVertex(DataType.BaseType.RELATION, type));
        relation.setHash(null);
        relation.type(type);
        return relation;
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
    public Type getType(String id) {
        return validConceptOfType(getConcept(id), TypeImpl.class);
    }

    @Override
    public Instance getInstance(String id) {
        return validConceptOfType(getConcept(id), InstanceImpl.class);
    }

    @Override
    public Entity getEntity(String id) {
        return validConceptOfType(getConcept(id), EntityImpl.class);
    }

    @Override
    public <V> Resource<V> getResource(String id) {
        return validConceptOfType(getConcept(id), ResourceImpl.class);
    }

    @Override
    public <V> Collection<Resource<V>> getResourcesByValue(V value) {
        HashSet<Resource<V>> resources = new HashSet<>();
        Data dataType = Data.SUPPORTED_TYPES.get(value.getClass().getTypeName());

        getTinkerTraversal().V().has(dataType.getConceptProperty().name(), value).
                forEachRemaining(v -> {
                    Concept resource = validConceptOfType(elementFactory.buildUnknownConcept(v), ResourceImpl.class);
                    if(resource != null && resource.isResource())
                        resources.add(resource.asResource());
                });

        return resources;
    }

    @Override
    public Rule getRule(String id) {
        return validConceptOfType(getConcept(id), RuleImpl.class);
    }

    @Override
    public EntityType getEntityType(String id) {
        return validConceptOfType(getConcept(id), EntityTypeImpl.class);
    }

    @Override
    public RelationType getRelationType(String id) {
        return validConceptOfType(getConcept(id), RelationTypeImpl.class);
    }

    @Override
    public <V> ResourceType<V> getResourceType(String id) {
        return validConceptOfType(getConcept(id), ResourceTypeImpl.class);
    }

    @Override
    public RoleType getRoleType(String id) {
        return validConceptOfType(getConcept(id), RoleTypeImpl.class);
    }

    @Override
    public RuleType getRuleType(String id) {
        return validConceptOfType(getConcept(id), RuleTypeImpl.class);
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

    public void handleTransaction(Consumer<Transaction> method){
        try {
            method.accept(getTinkerPopGraph().tx());
        } catch (UnsupportedOperationException e){
            LOG.warn(ErrorMessage.TRANSACTIONS_NOT_SUPPORTED.getMessage(graph.getClass().getName()));
        }
    }

    @Override
    public void refresh() {
        handleTransaction(Transaction::rollback);
        getConceptLog().clearTransaction();
    }

    /**
     * Closes the current transaction.
     */
    @Override
    public void close() {
        handleTransaction(Transaction::rollback);
        getConceptLog().clearTransaction();
        try {
            graph.tx().close();
        } catch(UnsupportedOperationException e) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Commits the graph
     * @throws MindmapsValidationException when the graph does not conform to the object model
     */
    @Override
    public void commit() throws MindmapsValidationException {
        validateGraph();

        Map<DataType.BaseType, Set<String>> modifiedConcepts = new HashMap<>();
        Set<String> castings = getModifiedCastingIds();

        if(castings.size() > 0)
            modifiedConcepts.put(DataType.BaseType.CASTING, castings);

        LOG.info("Graph is valid. Committing graph . . . ");
        handleTransaction(Transaction::commit);

        try {
            refresh();
        } catch (Exception e) {
            LOG.error("Failed to create new graph after committing", e);
            e.printStackTrace();
        }

        LOG.info("Graph committed.");

        if(modifiedConcepts.size() > 0)
            submitCommitLogs(modifiedConcepts);
    }

    protected void validateGraph() throws MindmapsValidationException {
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

        String result = EngineCommunicator.contactEngine(getCommitLogEndPoint(), "POST", postObject.toString());
        LOG.info("Response from engine [" + result + "]");
    }
    protected String getCommitLogEndPoint(){
        if(engine == null)
            return null;
        return engine + RESTUtil.WebPath.COMMIT_LOG_URI + "?" + RESTUtil.Request.GRAPH_NAME_PARAM + "=" + keyspace;
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
