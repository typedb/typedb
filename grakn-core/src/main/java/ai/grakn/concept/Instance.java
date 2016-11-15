/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.concept;

import java.util.Collection;

/**
 * This represents an instance of a Type. It represents data in the graph.
 */
public interface Instance extends Concept{
    //------------------------------------- Accessors ----------------------------------
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

    /**
     * Creates a relation from this instance to the provided resource.
     * @param resource The resource to creating a relationship to
     * @return A relation which contains both the entity and the resource
     */
    Relation hasResource(Resource resource);

    /**
     *
     * @param resourceTypes Resource Types of the resources attached to this entity
     * @return A collection of resources attached to this Instance.
     */
    Collection<Resource<?>> resources(ResourceType ... resourceTypes);
}
