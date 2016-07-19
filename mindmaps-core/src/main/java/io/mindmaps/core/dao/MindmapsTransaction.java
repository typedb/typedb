package io.mindmaps.core.dao;

import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.implementation.Data;
import io.mindmaps.core.model.*;

import java.util.Collection;
import java.util.Map;

/**
 * A thread bound mindmaps transaction
 */
public interface MindmapsTransaction {
    //------------------------------------- Concept Construction ----------------------------------

    /**
     *
     * @param id A unique Id for the Entity Type
     * @return A new or existing Entity Type with the provided Id.
     */
    EntityType putEntityType(String id);

    /**
     *
     * @param id A unique Id for the Resource Type
     * @param type The data type of the resource type.
     *             Supported types include: Data.STRING, Data.LONG, Data.DOUBLE, and Data.BOOLEAN
     * @param <V> The data type of the resource type. Supported types include: String, Long, Double, Boolean.
     *           This should match the parameter type
     * @return A new or existing Resource Type with the provided Id.
     */
    <V> ResourceType <V> putResourceType(String id, Data<V> type);

    /**
     *
     * @param id A unique Id for the Rule Type
     * @return new or existing Rule Type with the provided Id.
     */
    RuleType putRuleType(String id);

    /**
     *
     * @param id A unique Id for the Relation Type
     * @return A new or existing Relation Type with the provided Id.
     */
    RelationType putRelationType(String id);

    /**
     *
     * @param id A unique Id for the Role Type
     * @return new or existing Role Type with the provided Id.
     */
    RoleType putRoleType(String id);

    /**
     *
     * @param id A unique id for the Entity
     * @param type The type of this Entity
     * @return A new or existing Entity with the provided Id.
     */
    Entity putEntity(String id, EntityType type);

    /**
     *
     * @param type The type of this Entity
     * @return A new entity.
     */
    Entity addEntity(EntityType type);

    /**
     *
     * @param id A unique Id for the Resource
     * @param type The resource type of this resource.
     * @param <V> The data type of both the ResourceType and the Resource.
     *           Supported types include: String, Long, Double, Boolean.
     * @return new or existing Resource with the provided Id.
     */
    <V> Resource <V> putResource(String id, ResourceType<V> type);

    /**
     *
     * @param type The resource type of this resource.
     * @param <V> The data type of both the ResourceType and the Resource.
     *           Supported types include: String, Long, Double, Boolean.
     * @return a new resource.
     */
    <V> Resource <V> addResource(ResourceType<V> type);

    /**
     *
     * @param id A unique Id for the Rule
     * @param type The rule type of this Rule
     * @return new or existing Rule with the provided Id.
     */
    Rule putRule(String id, RuleType type);

    /**
     *
     * @param type The rule type of this Rule
     * @return a new Rule
     */
    Rule addRule(RuleType type);

    /**
     *
     * @param id A unique Id for the Relation
     * @param type The relation type of this Relation
     * @return A new empty relation which can be fully customised
     */
    Relation putRelation(String id, RelationType type);

    /**
     * 
     * @param type The relation type of this Relation
     * @param roleMap A role map specifying the rolePlayers (Instances or Resources) in the relationship and the roles (Role Types) they play.
     * @return A new or existing relation with the provided type and role map.
     */
    Relation putRelation(RelationType type, Map<RoleType, Instance> roleMap);

    /**
     *
     * @param type The relation type of this Relation
     * @return A new empty relation which can be fully customised
     */
    Relation addRelation(RelationType type);
    //------------------------------------- Concept Lookup ----------------------------------
    //------------------------------------- Concept
    /**
     *
     * @param id A unique Id which identifies the Concept in the graph.
     * @return The Concept with the provided Id or null if no such Concept exists.
     */
    Concept getConcept(String id);

    /**
     *
     * @param subject A unique subject Id which identifies the Concept in the graph.
     * @return The Concept with the provided Id or null if no such Concept exists.
     */
    Concept getConceptBySubject(String subject);

    /**
     *
     * @param value A value which a Concept in the graph may be holding.
     * @return The Concepts holding the provided value or an empty collection if no such Concept exists.
     */
    <V> Collection<Concept> getConceptsByValue(V value);

    //------------------------------------- Type
    /**
     *
     * @param id A unique Id which identifies the Type in the graph.
     * @return The Type with the provided Id or null if no such Type exists.
     */
    Type getType(String id);

    /**
     *
     * @param subject A unique subject Id which identifies the Type in the graph.
     * @return The Type with the provided Id or null if no such Type exists.
     */
    Type getTypeBySubject(String subject);

    /**
     *
     * @param value A value which a Type in the graph may be holding.
     * @return The Entities holding the provided value or an empty collection if no such Type exists.
     */
    Collection<Type> getTypesByValue(String value);

    //------------------------------------- Instance
    /**
     *
     * @param id A unique Id which identifies the Instance in the graph.
     * @return The Instance with the provided Id or null if no such Instance exists.
     */
    Instance getInstance(String id);

    /**
     *
     * @param subject A unique subject Id which identifies the Instance in the graph.
     * @return The Instance with the provided Id or null if no such Instance exists.
     */
    Instance getInstanceBySubject(String subject);

    /**
     *
     * @param value A value which a Instance in the graph may be holding.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Entities holding the provided value or an empty collection if no such Instance exists.
     */
    <V> Collection<Instance> getInstancesByValue(V value);


    //------------------------------------- Entity
    /**
     *
     * @param id A unique Id which identifies the Entity in the graph.
     * @return The Entity with the provided Id or null if no such Entity exists.
     */
    Entity getEntity(String id);

    /**
     *
     * @param subject A unique subject Id which identifies the Entity in the graph.
     * @return The Entity with the provided Id or null if no such Entity exists.
     */
    Entity getEntityBySubject(String subject);

    /**
     *
     * @param value A value which a Entity in the graph may be holding.
     * @return The Entities holding the provided value or an empty collection if no such Entity exists.
     */
    Collection<Entity> getEntitiesByValue(String value);
    //------------------------------------- Resource
    /**
     *
     * @param id A unique Id which identifies the Resource in the graph.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resource with the provided Id or null if no such Resource exists.
     */
    <V> Resource<V> getResource(String id);

    /**
     *
     * @param subject A unique subject Id which identifies the Resource in the graph.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resource with the provided Id or null if no such Resource exists.
     */
    <V> Resource<V> getResourceBySubject(String subject);

    /**
     *
     * @param value A value which a Resource in the graph may be holding.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resources holding the provided value or an empty collection if no such Resource exists.
     */
    <V> Collection<Resource<V>> getResourcesByValue(V value);
    //------------------------------------- Rule

    /**
     *
     * @param id A unique Id which identifies the Rule in the graph.
     * @return The Rule with the provided Id or null if no such Rule exists.
     */
    Rule getRule(String id);

    /**
     *
     * @param subject A unique subject Id which identifies the Rule in the graph.
     * @return The Rule with the provided Id or null if no such Rule exists.
     */
    Rule getRuleBySubject(String subject);

    /**
     *
     * @param value A value which a Rule in the graph may be holding.
     * @return The Rules holding the provided value or an empty collection if no such Rule exists.
     */
    Collection<Rule> getRulesByValue(String value);
    //------------------------------------- Concept Type
    /**
     *
     * @param id A unique Id which identifies the Entity Type in the graph.
     * @return The Entity Type  with the provided Id or null if no such Entity Type  exists.
     */
    EntityType getEntityType(String id);

    /**
     *
     * @param subject A unique subject Id which identifies the Entity Type  in the graph.
     * @return The Entity Type  with the provided Id or null if no such Entity Type  exists.
     */
    EntityType getEntityTypeBySubject(String subject);

    /**
     *
     * @param value A value which a Entity Type  in the graph may be holding.
     * @return The Entity Types holding the provided value or an empty collection if no such Entity Type  exists.
     */
    Collection<EntityType> getEntityTypesByValue(String value);
    //------------------------------------- Relation Type
    /**
     *
     * @param id A unique Id which identifies the Relation Type in the graph.
     * @return The Relation Type with the provided Id or null if no such Relation Type exists.
     */
    RelationType getRelationType(String id);

    /**
     *
     * @param subject A unique subject Id which identifies the Relation Type in the graph.
     * @return The Relation Type with the provided Id or null if no such Relation Type exists.
     */
    RelationType getRelationTypeBySubject(String subject);

    /**
     *
     * @param value A value which a Relation Type in the graph may be holding.
     * @return The Relation Types holding the provided value or an empty collection if no such Relation Type exists.
     */
    Collection<RelationType> getRelationTypesByValue(String value);
    //------------------------------------- Resource Type
    /**
     *
     * @param id A unique Id which identifies the Resource Type in the graph.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resource Type with the provided Id or null if no such Resource Type exists.
     */
    <V> ResourceType<V> getResourceType(String id);

    /**
     *
     * @param subject A unique subject Id which identifies the Resource Type in the graph.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resource Type with the provided Id or null if no such Resource Type exists.
     */
    <V> ResourceType<V> getResourceTypeBySubject(String subject);

    /**
     *
     * @param value A value which a Resource Type in the graph may be holding.
     * @param <V> The data type of the value. Supported types include: String, Long, Double, and Boolean.
     * @return The Resource Types holding the provided value or an empty collection if no such Resource Type exists.
     */
    <V> Collection<ResourceType<V>> getResourceTypesByValue(String value);
    //------------------------------------- Role Type
    /**
     *
     * @param id A unique Id which identifies the Role Type in the graph.
     * @return The Role Type  with the provided Id or null if no such Role Type exists.
     */
    RoleType getRoleType(String id);

    /**
     *
     * @param subject A unique subject Id which identifies the Role Type in the graph.
     * @return The Role Type with the provided Id or null if no such Role Type exists.
     */
    RoleType getRoleTypeBySubject(String subject);

    /**
     *
     * @param value A value which a Role Type in the graph may be holding.
     * @return The Role Types holding the provided value or an empty collection if no such Role Type exists.
     */
    Collection<RoleType> getRoleTypesByValue(String value);
    //------------------------------------- Rule Type
    /**
     *
     * @param id A unique Id which identifies the Rule Type in the graph.
     * @return The Rule Type with the provided Id or null if no such Rule Type exists.
     */
    RuleType getRuleType(String id);

    /**
     *
     * @param subject A unique subject Id which identifies the Rule Type in the graph.
     * @return The Rule Type with the provided Id or null if no such Rule Type  exists.
     */
    RuleType getRuleTypeBySubject(String subject);

    /**
     *
     * @param value A value which a Rule Type in the graph may be holding.
     * @return The Rule Types holding the provided value or an empty collection if no such Rule Type exists.
     */
    Collection<RuleType> getRuleTypesByValue(String value);

    //------------------------------------- Relationship Handling ----------------------------------
    /**
     *
     * @return The meta type -> type. The root of all Types.
     */
    Type getMetaType();

    /**
     *
     * @return The meta relation type -> relation-type. The root of all Relation Types.
     */
    Type getMetaRelationType();

    /**
     *
     * @return The meta role type -> role-type. The root of all the Role Types.
     */
    Type getMetaRoleType();

    /**
     *
     * @return The meta resource type -> resource-type. The root of all the Resource Types.
     */
    Type getMetaResourceType();

    /**
     *
     * @return The meta entity type -> entity-type. The root of all the Entity Types.
     */
    Type getMetaEntityType();

    /**
     *
     * @return The meta rule type -> rule-type. The root of all the Rule Types.
     */
    Type getMetaRuleType();

    /**
     *
     * @return The meta rule -> inference-rule.
     */
    RuleType getMetaRuleInference();

    /**
     *
     * @return The meta rule -> constraint-rule.
     */
    RuleType getMetaRuleConstraint();

    /**
     *
     * @param relationType The Relation Type which we wish to find a Relation instance of.
     * @param roleMap A role map specifying the rolePlayers (Instances or Resources) in the relationship and the roles (Role Types) they play.
     * @return A collection of Relations which meet the above requirements or an empty collection is no relationship exists fulfilling the above requirements.
     */
    Relation getRelation(RelationType relationType, Map<RoleType, Instance> roleMap);

    /**
     *
     * @param id The id of the relation object you are looking for
     * @return The relation object.
     */
    Relation getRelation(String id);
    //------------------------------------- Utilities ----------------------------------
    /**
     * Validates and attempts to commit the graph. An exception is thrown if validation fails or if the graph cannot be persisted due to an underlying database issue.
     * @throws MindmapsValidationException is thrown when a structural validation fails.
     */
    void commit() throws MindmapsValidationException;

    /**
     * Resets the current transaction without commiting.
     * @throws Exception
     */
    void refresh() throws Exception;

    /**
     * Closes the current transaction rendering it unusable.
     * @throws Exception
     */
    void close() throws Exception;

    /**
     * Resets the graph to an empty state. A commit is required for this to take affect.
     */
    void clearGraph();

    /**
     * Enables batch loading which skips redundancy checks.
     * With this mode enabled duplicate concepts and relations maybe created.
     * Faster writing at the cost of consistency.
     */
    void enableBatchLoading();

    /**
     * Disables batch loading which prevents the creation of duplicate castings.
     * Immediate constancy at the cost of writing speed.
     */
    void disableBatchLoading();

    /**
     *
     * @return A flag indicating if this transaction is batch loading or not
     */
    boolean isBatchLoadingEnabled();
}
