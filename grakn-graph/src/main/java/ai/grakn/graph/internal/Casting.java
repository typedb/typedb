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

package ai.grakn.graph.internal;

import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeId;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;

/**
 * <p>
 *     Represents An Instance Playing a Role
 * </p>
 *
 * <p>
 *    Wraps the shortcut {@link Edge} which contains the information unifying an {@link Instance},
 *    {@link Relation} and {@link RoleType}.
 * </p>
 *
 * @author fppt
 */
class Casting {
    private final EdgeElement edgeElement;
    private final Cache<RoleType> cachedRoleType = new Cache<>(() -> (RoleType) edge().graph().getType(TypeId.of(edge().property(Schema.EdgeProperty.ROLE_TYPE_ID))));
    private final Cache<RelationType> cachedRelationType = new Cache<>(() -> (RelationType) edge().graph().getType(TypeId.of(edge().property(Schema.EdgeProperty.RELATION_TYPE_ID))));
    private final Cache<Instance> cachedInstance = new Cache<>(() -> edge().graph().factory().buildConcept(edge().getTarget()));
    private final Cache<Relation> cachedRelation = new Cache<>(() -> edge().graph().factory().buildConcept(edge().getSource()));

    Casting(EdgeElement edgeElement){
        this.edgeElement = edgeElement;
    }

    EdgeElement edge(){
        return edgeElement;
    }

    /**
     *
     * @return The role the instance is playing
     */
    public RoleType getRoleType(){
        return cachedRoleType.get();
    }

    /**
     *
     * @return The relation type the instance is taking part in
     */
    public RelationType getRelationType(){
        return cachedRelationType.get();
    }

    /**
     *
     * @return The relation which is linking the role and the instance
     */
    public Relation getRelation(){
        return cachedRelation.get();
    }

    /**
     *
     * @return The instance playing the role
     */
    public Instance getInstance(){
        return cachedInstance.get();
    }

    /**
     *
     * @return The hash code of the underlying vertex
     */
    public int hashCode() {
        return edge().id().hashCode();
    }

    /**
     *
     * @return true if the elements equal each other
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        Casting casting = (Casting) object;

        return edge().id().equals(casting.edge().id());
    }
}
