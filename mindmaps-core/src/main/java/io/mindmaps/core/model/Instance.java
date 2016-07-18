package io.mindmaps.core.model;

import java.util.Collection;

/**
 * This represents an instance of a Type. It represents data in the graph.
 */
public interface Instance extends Concept{
    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @param id The new unique id of the instance.
     * @return The instance itself.
     */
    Instance setId(String id);

    /**
     *
     * @param subject The new unique subject of the instance.
     * @return The instance itself.
     */
    Instance setSubject(String subject);

    /**
     *
     * @param roleTypes An optional parameter which allows you to specify the role of the relations you wish to retrieve.
     * @return A set of Relations which the concept instance takes part in, optionally constrained by the Role Type.
     */
    Collection<Relation> relations(RoleType... roleTypes);

    /**
     *
     * @return A set of all the Role Types which this instance plays.
     */
    Collection<RoleType> playsRoles();
}
