/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.server.kb.concept;

import grakn.core.server.keyspace.Keyspace;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.LabelId;
import grakn.core.graql.concept.Relationship;
import grakn.core.graql.concept.RelationshipType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Thing;
import grakn.core.server.kb.cache.Cache;
import grakn.core.server.kb.cache.CacheOwner;
import grakn.core.server.kb.cache.Cacheable;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.graql.internal.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * <p>
 *     Encapsulates The {@link Relationship} as a {@link EdgeElement}
 * </p>
 *
 * <p>
 *     This wraps up a {@link Relationship} as a {@link EdgeElement}. It is used to represent any binary {@link Relationship}.
 *     This also includes the ability to automatically reify a {@link RelationshipEdge} into a {@link RelationshipReified}.
 * </p>
 *
 *
 */
public class RelationshipEdge implements RelationshipStructure, CacheOwner {
    private final Set<Cache> registeredCaches = new HashSet<>();
    private final Logger LOG = LoggerFactory.getLogger(RelationshipEdge.class);
    private final EdgeElement edgeElement;

    private final Cache<RelationshipType> relationType = Cache.createTxCache(this, Cacheable.concept(), () ->
            edge().tx().getSchemaConcept(LabelId.of(edge().property(Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID))));

    private final Cache<Role> ownerRole = Cache.createTxCache(this, Cacheable.concept(), () -> edge().tx().getSchemaConcept(LabelId.of(
            edge().property(Schema.EdgeProperty.RELATIONSHIP_ROLE_OWNER_LABEL_ID))));

    private final Cache<Role> valueRole = Cache.createTxCache(this, Cacheable.concept(), () -> edge().tx().getSchemaConcept(LabelId.of(
            edge().property(Schema.EdgeProperty.RELATIONSHIP_ROLE_VALUE_LABEL_ID))));

    private final Cache<Thing> owner = Cache.createTxCache(this, Cacheable.concept(), () ->
            edge().tx().factory().<Thing>buildConcept(edge().source())
    );

    private final Cache<Thing> value = Cache.createTxCache(this, Cacheable.concept(), () ->
            edge().tx().factory().<Thing>buildConcept(edge().target())
    );

    private RelationshipEdge(EdgeElement edgeElement) {
        this.edgeElement = edgeElement;
    }

    private RelationshipEdge(RelationshipType relationshipType, Role ownerRole, Role valueRole, EdgeElement edgeElement) {
        this(edgeElement);

        edgeElement.propertyImmutable(Schema.EdgeProperty.RELATIONSHIP_ROLE_OWNER_LABEL_ID, ownerRole, null, o -> o.labelId().getValue());
        edgeElement.propertyImmutable(Schema.EdgeProperty.RELATIONSHIP_ROLE_VALUE_LABEL_ID, valueRole, null, v -> v.labelId().getValue());
        edgeElement.propertyImmutable(Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID, relationshipType, null, t -> t.labelId().getValue());

        this.relationType.set(relationshipType);
        this.ownerRole.set(ownerRole);
        this.valueRole.set(valueRole);
    }

    public static RelationshipEdge get(EdgeElement edgeElement){
        return new RelationshipEdge(edgeElement);
    }

    public static RelationshipEdge create(RelationshipType relationshipType, Role ownerRole, Role valueRole, EdgeElement edgeElement) {
        return new RelationshipEdge(relationshipType, ownerRole, valueRole, edgeElement);
    }

    private EdgeElement edge(){
        return edgeElement;
    }

    @Override
    public ConceptId id() {
        return ConceptId.of(edge().id().getValue());
    }

    @Override
    public Keyspace keyspace() {
        return edge().tx().keyspace();
    }

    @Override
    public RelationshipReified reify() {
        LOG.debug("Reifying concept [" + id() + "]");
        //Build the Relationship Vertex
        VertexElement relationVertex = edge().tx().addVertexElement(Schema.BaseType.RELATIONSHIP, id());
        RelationshipReified relationReified = edge().tx().factory().buildRelationReified(relationVertex, type());

        //Delete the old edge
        delete();

        return relationReified;
    }

    @Override
    public boolean isReified() {
        return false;
    }

    @Override
    public RelationshipType type() {
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
    public Stream<Thing> rolePlayers(Role... roles) {
        if(roles.length == 0){
            return Stream.of(owner(), value());
        }

        HashSet<Thing> result = new HashSet<>();
        for (Role role : roles) {
            if(role.equals(ownerRole())) {
                result.add(owner());
            } else if (role.equals(valueRole())) {
                result.add(value());
            }
        }
        return result.stream();
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
    public boolean isDeleted() {
        return edgeElement.isDeleted();
    }

    @Override
    public boolean isInferred() {
        return edge().propertyBoolean(Schema.EdgeProperty.IS_INFERRED);
    }

    @Override
    public String toString(){
        return "ID [" + id() + "] Type [" + type().label() + "] Roles and Role Players: \n" +
                "Role [" + ownerRole().label() + "] played by [" + owner().id() + "] \n" +
                "Role [" + valueRole().label() + "] played by [" + value().id() + "] \n";
    }

    @Override
    public Collection<Cache> caches() {
        return registeredCaches;
    }
}
