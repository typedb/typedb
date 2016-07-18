package io.mindmaps.core.model;

import io.mindmaps.core.implementation.Data;

import java.util.Collection;

/**
 * A concept which represents a resource.
 * @param <D> The data type of this resource. Supported Types include: String, Long, Double, and Boolean
 */
public interface Resource<D> extends Instance{
    //------------------------------------- Modifiers ----------------------------------
    /**
     *
     * @param id The new unique id of the instance.
     * @return The Resource itself
     */
    Resource<D> setId(String id);

    /**
     *
     * @param subject The new unique subject of the instance.
     * @return The Resource itself
     */
    Resource<D> setSubject(String subject);

    /**
     *
     * @param value The value to store on the resource
     * @return The Resource itself
     */
    Resource<D> setValue(D value);

    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return The Resource itself
     */
    D getValue();

    /**
     *
     * @return the type of this resource
     */
    ResourceType<D> type();

    /**
     *
     * @return The data type of this Resource's type.
     */
    Data<D> dataType();

    /**
     * @return The list of all Instances which posses this resource
     */
    Collection<Instance> ownerInstances();

}
