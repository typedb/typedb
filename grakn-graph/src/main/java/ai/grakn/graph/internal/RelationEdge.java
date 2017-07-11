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

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.util.Schema;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 *     Encapsulates The {@link Relation} as a {@link EdgeElement}
 * </p>
 *
 * <p>
 *     This wraps up a {@link Relation} as a {@link EdgeElement}. It is used to represent any binary {@link Relation}.
 * </p>
 *
 * @author fppt
 *
 */
class RelationEdge implements RelationStructure{
    private final AbstractGraknGraph<?> graph;
    private final EdgeElement edgeElement;

    public RelationEdge(AbstractGraknGraph<?> graph, EdgeElement edgeElement) {
        this.graph = graph;
        this.edgeElement = edgeElement;
    }

    EdgeElement edge(){
        return edgeElement;
    }

    @Override
    public ConceptId getId() {
        return ConceptId.of(edge().id().getValue());
    }

    @Override
    public RelationReified reify() {
        throw new UnsupportedOperationException("Reification is not yet supported");
    }

    @Override
    public boolean isReified() {
        return false;
    }

    @Override
    public RelationType type() {
        LabelId labelId = LabelId.of(edge().property(Schema.EdgeProperty.RELATION_TYPE_ID));
        return graph.getOntologyConcept(labelId);
    }

    @Override
    public Map<Role, Set<Thing>> allRolePlayers() {
        return null;
    }

    @Override
    public Collection<Thing> rolePlayers(Role... roles) {
        return null;
    }

    @Override
    public void delete() {

    }
}
