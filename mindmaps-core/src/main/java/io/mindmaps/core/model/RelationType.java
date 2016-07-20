package io.mindmaps.core.model;

import java.util.Collection;

public interface RelationType extends Type {
    //------------------------------------- Accessors ----------------------------------
    /**
     *
     * @return A list of the Role Types which make up this Relation Type.
     */
    Collection<RoleType> hasRoles();

    //------------------------------------- Edge Handling ----------------------------------

    /**
     *
     * @param roleType A new role which is part of this relationship.
     * @return The Relation Type itself.
     */
    RelationType hasRole(RoleType roleType);

    //------------------------------------- Other ----------------------------------

    /**
     *
     * @param roleType The role type to delete from this relationship.
     * @return The Relation Type itself.
     */
    RelationType deleteHasRole(RoleType roleType);

    //---- Inherited Methods
    RelationType setId(String id);
    RelationType setSubject(String subject);
    RelationType setValue(String value);
    RelationType setAbstract(Boolean isAbstract);
    RelationType superType();
    RelationType superType(RelationType type);
    Collection<RelationType> subTypes();
    RelationType playsRole(RoleType roleType);
    RelationType deletePlaysRole(RoleType roleType);
    Collection<Relation> instances();
}
