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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private final EdgeElement edgeElement;

    private Cache<Role> ownerRole = new Cache<>(() -> edge().graph().getOntologyConcept(LabelId.of(
            edge().property(Schema.EdgeProperty.RELATION_ROLE_OWNER_LABEL_ID))));

    private Cache<Role> valueRole = new Cache<>(() -> edge().graph().getOntologyConcept(LabelId.of(
            edge().property(Schema.EdgeProperty.RELATION_ROLE_VALUE_LABEL_ID))));

    private Cache<Thing> owner = new Cache<>(() -> edge().graph().factory().buildConcept(edge().source()));
    private Cache<Thing> value = new Cache<>(() -> edge().graph().factory().buildConcept(edge().target()));

    public RelationEdge(EdgeElement edgeElement) {
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
        return edge().graph().getOntologyConcept(LabelId.of(edge().property(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID)));
    }

    @Override
    public Map<Role, Set<Thing>> allRolePlayers() {
        HashMap<Role, Set<Thing>> result = new HashMap<>();
        result.put(ownerRole(), Collections.singleton(owner()));
        result.put(valueRole(), Collections.singleton(value()));
        return result;
    }

    @Override
    public Collection<Thing> rolePlayers(Role... roles) {
        if(roles.length == 0){
            return Stream.of(owner(), value()).collect(Collectors.toSet());
        }

        HashSet<Thing> result = new HashSet<>();
        for (Role role : roles) {
            if(role.equals(ownerRole())) {
                result.add(owner());
            } else if (role.equals(valueRole())) {
                result.add(value());
            }
        }
        return result;
    }

    private Role ownerRole(){
        return ownerRole.get();
    }
    private Thing owner(){
        return owner.get();
    }

    private Role valueRole(){
        return valueRole.get();
    }
    private Thing value(){
        return value.get();
    }

    @Override
    public void delete() {
        edge().delete();
    }
}
