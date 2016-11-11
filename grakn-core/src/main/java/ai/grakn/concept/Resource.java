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
 * A concept which represents a resource.
 * @param <D> The data type of this resource. Supported Types include: String, Long, Double, and Boolean
 */
public interface Resource<D> extends Instance{
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
    ResourceType.DataType<D> dataType();

    /**
     * @return The list of all Instances which posses this resource
     */
    Collection<Instance> ownerInstances();

    /**
     *
     * @return The instance which is connected to this unique resource, if the resource is unique
     */
    Instance owner();

}
