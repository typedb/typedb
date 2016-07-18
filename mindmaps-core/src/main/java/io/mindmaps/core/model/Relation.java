package io.mindmaps.core.model;


import io.mindmaps.core.exceptions.ConceptException;

import java.util.Collection;
import java.util.Map;

public interface Relation extends Instance {
    //------------------------------------- Modifiers ----------------------------------
    /**
     * This Method will soon be deprecated.
     * @param id The new unique id of the Relation.
     * @return The Relation itself
     */
    Relation setId(String id);

    /**
     *
     * @param subject The new unique subject of the Relation.
     * @return The Relation itself
     */
    Relation setSubject(String subject);

    /**
     *
     * @param value The String value of the relation
     * @return The Relation itself
     */
    Relation setValue(String value);

    /**
     *
     * @param instance A new instance which can scope this Relation
     * @return The Relation itself
     */
    Relation scope(Instance instance);

    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return The String value of the relation
     */
    String getValue();

    /**
     *
     * @return The Relation type of this Relation.
     */
    RelationType type();

    /**
     *
     * @return A list of all the Instances involved in the relationships and the Role Types which they play.
     */
    Map<RoleType, Instance> rolePlayers();

    /**
     *
     * @return A collection of resources attached to this Instance.
     */
    Collection<Resource<?>> resources();

    /**
     *
     * @return A list of the Instances which scope this Relation
     */
    Collection<Instance> scopes();

    /**
     * Expands this Relation to include a new role player which is playing a specific role.
     * @param roleType The role of the new role player.
     * @param instance The new role player.
     * @return The Relation itself
     */
    Relation putRolePlayer(RoleType roleType, Instance instance);

    //------------------------------------- Other ----------------------------------

    /**
     * @param scope A concept which is currently scoping this concept.
     * @return The Relation itself
     */
    Relation deleteScope(Instance scope) throws ConceptException;
}
