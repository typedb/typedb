package io.mindmaps.core.model;

import java.util.Collection;

/**
 * An ontological element which represents the categories instances can fall within.
 */
public interface EntityType extends Type{
    //------------------------------------- Modifiers ----------------------------------

    /**
     *
     * @param id The new unique id of the Entity Type.
     * @return The Entity Type itself.
     */
    EntityType setId(String id);

    /**
     *
     * @param subject The new unique subject of the Entity Type.
     * @return The Entity Type itself.
     */
    EntityType setSubject(String subject);

    /**
     *
     * @param value The String value to store in the Entity Type
     * @return The Entity Type itself
     */
    EntityType setValue(String value);

    /**
     *
     * @param isAbstract  Specifies if the concept is abstract (true) or not (false).
     *                    If the concept type is abstract it is not allowed to have any instances.
     * @return The Entity Type itself
     */
    EntityType setAbstract(Boolean isAbstract);

    /**
     *
     * @param type The super of this Entity Type
     * @return The Entity Type itself
     */
    EntityType superType(EntityType type);

    /**
     *
     * @param roleType The Role Type which the instances of this Entity Type are allowed to play.
     * @return The Entity Type itself
     */
    EntityType playsRole(RoleType roleType);

    /**
     *
     * @param roleType The Role Type which the instances of this Entity Type should no longer be allowed to play.
     * @return The Entity Type itself
     */
    EntityType deletePlaysRole(RoleType roleType);

    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return The super of this Entity Type
     */
    EntityType superType();

    /**
     *
     * @return All the sub classes of this Entity Type
     */
    Collection<EntityType> subTypes();

    /**
     *
     * @return All the instances of this Entity Type.
     */
    Collection<Entity> instances();
}
