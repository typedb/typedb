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
 * <p>
 *     A data instance in the graph belonging to a specific {@link Type}
 * </p>
 *
 * <p>
 *     Instances represent data in the graph.
 *     Every instance belongs to a {@link Type} which serves as a way of categorising them.
 *     Instances can relate to one another via {@link Relation}
 * </p>
 *
 * @see Entity
 * @see Relation
 * @see Resource
 * @see Rule
 *
 * @author fppt
 *
 */
public interface Instance extends Concept{
    //------------------------------------- Accessors ----------------------------------
    /**
     * Return the Type of the Concept.
     *
     * @return A Type which is the type of this concept. This concept is an instance of that type.
     */
    Type type();

    /**
     * Retrieves a Relations which the Instance takes part in, which may optionally be narrowed to a particular set
     * according to the RoleType you are interested in.
     * @see RoleType
     * @see Relation
     *
     * @param roleTypes An optional parameter which allows you to specify the role of the relations you wish to retrieve.
     * @return A set of Relations which the concept instance takes part in, optionally constrained by the Role Type.
     */
    Collection<Relation> relations(RoleType... roleTypes);

    /**
     * Determine the Role Types that this Instance may play.
     * @see RoleType
     *
     * @return A set of all the Role Types which this instance plays.
     */
    Collection<RoleType> plays();

    /**
     * Creates a relation from this instance to the provided resource.
     *
     * @param resource The resource to which a relationship is created
     * @return The instance itself
     */
    Instance resource(Resource resource);

    /**
     * Retrieves a collection of Resources attached to this Instances
     * @see Resource
     *
     * @param resourceTypes Resource Types of the resources attached to this entity
     * @return A collection of resources attached to this Instance.
     */
    Collection<Resource<?>> resources(ResourceType ... resourceTypes);
}
