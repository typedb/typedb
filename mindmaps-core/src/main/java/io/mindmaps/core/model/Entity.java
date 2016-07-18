package io.mindmaps.core.model;

import java.util.Collection;

/**
 * An instance of Entity Type which represents some data in the graph.
 */
public interface Entity extends Instance{
    //------------------------------------- Modifiers ----------------------------------
    /**
     *
     * @param id The new unique id of the instance.
     * @return The Entity itself
     */
    Entity setId(String id);

    /**
     *
     * @param subject The new unique subject of the instance.
     * @return The Entity itself
     */
    Entity setSubject(String subject);

    /**
     *
     * @param value The String value to store on this Entity
     * @return The Entity itself
     */
    Entity setValue(String value);

    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return The String value stored on this Entity
     */
    String getValue();

    /**
     *
     * @return The Entity Type of this Entity
     */
    EntityType type();

    /**
     *
     * @return A collection of resources attached to this Instance.
     */
    Collection<Resource<?>> resources();
}
