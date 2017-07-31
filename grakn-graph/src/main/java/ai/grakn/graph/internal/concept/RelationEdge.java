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

package ai.grakn.graph.internal.concept;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.LabelId;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.graph.internal.cache.Cache;
import ai.grakn.graph.internal.cache.Cacheable;
import ai.grakn.graph.internal.structure.EdgeElement;
import ai.grakn.graph.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 *     Encapsulates The {@link Relation} as a {@link EdgeElement}
 * </p>
 *
 * <p>
 *     This wraps up a {@link Relation} as a {@link EdgeElement}. It is used to represent any binary {@link Relation}.
 *     This also includes the ability to automatically reify a {@link RelationEdge} into a {@link RelationReified}.
 * </p>
 *
 * @author fppt
 *
 */
public class RelationEdge implements RelationStructure{
    private final EdgeElement edgeElement;

    private final Cache<RelationType> relationType = new Cache<>(Cacheable.concept(), () ->
            edge().graph().getOntologyConcept(LabelId.of(edge().property(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID))));

    private final Cache<Role> ownerRole = new Cache<>(Cacheable.concept(), () -> edge().graph().getOntologyConcept(LabelId.of(
            edge().property(Schema.EdgeProperty.RELATION_ROLE_OWNER_LABEL_ID))));

    private final Cache<Role> valueRole = new Cache<>(Cacheable.concept(), () -> edge().graph().getOntologyConcept(LabelId.of(
            edge().property(Schema.EdgeProperty.RELATION_ROLE_VALUE_LABEL_ID))));

    private final Cache<Thing> owner = new Cache<>(Cacheable.concept(), () -> edge().graph().factory().buildConcept(edge().source()));
    private final Cache<Thing> value = new Cache<>(Cacheable.concept(), () -> edge().graph().factory().buildConcept(edge().target()));

    RelationEdge(EdgeElement edgeElement) {
        this.edgeElement = edgeElement;
    }

    RelationEdge(RelationType relationType, Role ownerRole, Role valueRole, EdgeElement edgeElement) {
        this(edgeElement);

        edgeElement.propertyImmutable(Schema.EdgeProperty.RELATION_ROLE_OWNER_LABEL_ID, ownerRole, null, o -> o.getLabelId().getValue());
        edgeElement.propertyImmutable(Schema.EdgeProperty.RELATION_ROLE_VALUE_LABEL_ID, valueRole, null, v -> v.getLabelId().getValue());
        edgeElement.propertyImmutable(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID, relationType, null, t -> t.getLabelId().getValue());

        this.relationType.set(relationType);
        this.ownerRole.set(ownerRole);
        this.valueRole.set(valueRole);
    }

    private EdgeElement edge(){
        return edgeElement;
    }

    @Override
    public ConceptId getId() {
        return ConceptId.of(edge().id().getValue());
    }

    @Override
    public RelationReified reify() {
        //Build the Relation Vertex
        VertexElement relationVertex = edge().graph().addVertex(Schema.BaseType.RELATION, getId());
        RelationReified relationReified = edge().graph().factory().buildRelationReified(relationVertex, type());

        //Delete the old edge
        delete();

        return relationReified;
    }

    @Override
    public boolean isReified() {
        return false;
    }

    @Override
    public RelationType type() {
        return relationType.get();
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
            return Sets.newHashSet(owner(), value());
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

    @Override
    public void txCacheClear() {
        relationType.clear();
        ownerRole.clear();
        valueRole.clear();
        owner.clear();
        value.clear();
    }

    public Role ownerRole(){
        return ownerRole.get();
    }
    public Thing owner(){
        return owner.get();
    }

    public Role valueRole(){
        return valueRole.get();
    }
    public Thing value(){
        return value.get();
    }

    @Override
    public void delete() {
        edge().delete();
    }

    @Override
    public String toString(){
        return "ID [" + getId() + "] Type [" + type().getLabel() + "] Roles and Role Players: \n" +
                "Role [" + ownerRole().getLabel() + "] played by [" + owner().getId() + "] \n" +
                "Role [" + valueRole().getLabel() + "] played by [" + value().getId() + "] \n";
    }
}
