package io.mindmaps.core.model;

import io.mindmaps.core.exceptions.ConceptException;

/**
 * A Concept which represents anything in the graph.
 */
public interface Concept extends Comparable<Concept>{
    //------------------------------------- Modifiers ----------------------------------
    /**
     *
     * @param id The new unique id of the concept.
     * @return The concept itself.
     */
    Concept setId(String id);

    /**
     *
     * @param subject The new unique subject of the concept.
     * @return The concept itself.
     */
    Concept setSubject(String subject);

    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return A string representing the concept's unique id.
     */
    String getId();

    /**
     *
     * @return A string representing the concept's unique subject.
     */
    String getSubject();

    /**
     *
     * @return A Type which is the type of this concept. This concept is an instance of that type.
     */
    Type type();

    /**
     *
     * @return The value stored in the concept
     */
    Object getValue();

    //------------------------------------- Other ---------------------------------

    /**
     *
     * @return A Type if the concept is a Type
     */
    Type asType();

    /**
     *
     * @return An Instance if the concept is an Instance
     */
    Instance asInstance();

    /**
     *
     * @return A Entity Type if the concept is a Entity Type
     */
    EntityType asEntityType();

    /**
     *
     * @return A Role Type if the concept is a Role Type
     */
    RoleType asRoleType();

    /**
     *
     * @return A Relation Type if the concept is a Relation Type
     */
    RelationType asRelationType();

    /**
     *
     * @return A Resource Type if the concept is a Resource Type
     */
    <D> ResourceType<D> asResourceType();

    /**
     *
     * @return A Rule Type if the concept is a Rule Type
     */
    RuleType asRuleType();

    /**
     *
     * @return An Entity if the concept is an Instance
     */
    Entity asEntity();

    /**
     *
     * @return A Relation if the concept is a Relation
     */
    Relation asRelation();

    /**
     *
     * @return A Resource if the concept is a Resource
     */
    <D> Resource<D> asResource();

    /**
     *
     * @return A Rule if the concept is a Rule
     */
    Rule asRule();

    /**
     *
     * @return true if the concept is a Type
     */
    boolean isType();

    /**
     *
     * @return true if the concept is an Instance
     */
    boolean isInstance();

    /**
     *
     * @return true if the concept is a Entity Type
     */
    boolean isEntityType();

    /**
     *
     * @return true if the concept is a Role Type
     */
    boolean isRoleType();

    /**
     *
     * @return true if the concept is a Relation Type
     */
    boolean isRelationType();

    /**
     *
     * @return true if the concept is a Resource Type
     */
    boolean isResourceType();

    /**
     *
     * @return true if the concept is a Rule Type
     */
    boolean isRuleType();

    /**
     *
     * @return true if the concept is a Entity
     */
    boolean isEntity();

    /**
     *
     * @return true if the concept is a Relation
     */
    boolean isRelation();

    /**
     *
     * @return true if the concept is a Resource
     */
    boolean isResource();

    /**
     *
     * @return true if the concept is a Rule
     */
    boolean isRule();

    /**
     * Deletes the concept.
     * @throws ConceptException Throws an exception if the node has any edges attached to it.
     */
    void delete() throws ConceptException;
}