/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

import grakn.core.concept.ConceptId;
import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.Casting;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.kb.structure.VertexElement;
import grakn.core.server.session.ConceptObserver;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Encapsulates The Relation as a VertexElement
 * This wraps up a Relation as a VertexElement. It is used to represent any Relation which
 * has been reified.
 */
public class RelationReified extends ThingImpl<Relation, RelationType> implements RelationStructure {

    @Nullable
    private RelationImpl owner;

    RelationReified(VertexElement vertexElement, ConceptManager conceptManager, ConceptObserver conceptObserver) {
        super(vertexElement, conceptManager, conceptObserver);
    }

    @Override
    public void delete() {
        //TODO remove this once we fix the whole relation hierarchy
        // removing the owner as it is the real concept that gets cached.
        // trying to delete a RelationStructure will fail the concept.isRelation check leading to errors when deleting the relation from transactionCache
        conceptObserver.deleteReifiedOwner(owner);

        super.delete();
    }

    @Override
    public ConceptId id(){
        //if this is implicit, it's possible than we reified it - the concept id of the edge relation is contained in the property
        if (type().isImplicit()){
            VertexProperty<Object> edgeId = vertex().element().property(Schema.VertexProperty.EDGE_RELATION_ID.name());
            if (edgeId.isPresent()) return ConceptId.of(edgeId.value().toString());
        }
        return Schema.conceptId(vertex().element());
    }

    @Override
    public Map<Role, Set<Thing>> allRolePlayers() {
        HashMap<Role, Set<Thing>> roleMap = new HashMap<>();

        //We add the role types explicitly so we can return them when there are no roleplayers
        type().roles().forEach(roleType -> roleMap.put(roleType, new HashSet<>()));
        //All castings are used here because we need to iterate over all of them anyway
        castingsRelation().forEach(rp -> roleMap.computeIfAbsent(rp.getRole(), (k) -> new HashSet<>()).add(rp.getRolePlayer()));

        return roleMap;
    }

    @Override
    public Stream<Thing> rolePlayers(Role... roles) {
        return castingsRelation(roles).map(Casting::getRolePlayer).distinct();
    }

    void removeRolePlayer(Role role, Thing thing) {
        castingsRelation().filter(casting -> casting.getRole().equals(role) && casting.getRolePlayer().equals(thing)).
                findAny().
                ifPresent(casting -> {
                    casting.delete();
                    conceptObserver.castingDeleted(casting);
                });
    }

    public void addRolePlayer(Role role, Thing thing) {
        Objects.requireNonNull(role);
        Objects.requireNonNull(thing);

        if (Schema.MetaSchema.isMetaLabel(role.label())) throw TransactionException.metaTypeImmutable(role.label());

        //Do the actual put of the role and role player
        putRolePlayerEdge(role, thing);
    }

    /**
     * If the edge does not exist then it adds a Schema.EdgeLabel#ROLE_PLAYER edge from
     * this Relation to a target Thing which is playing some Role.
     * If the edge does exist nothing is done.
     *
     * @param role    The Role being played by the Thing in this Relation
     * @param toThing The Thing playing a Role in this Relation
     */
    public void putRolePlayerEdge(Role role, Thing toThing) {
        //Checking if the edge exists
        boolean rolePlayerEdgeExists = vertex()
                .rolePlayerEdgeExists(
                        elementId().toString(),
                        type(),
                        role,
                        ConceptVertex.from(toThing).elementId().toString()
                );

        if (rolePlayerEdgeExists) {
            return;
        }

        //Role player edge does not exist create a new one
        EdgeElement edge = this.addEdge(ConceptVertex.from(toThing), Schema.EdgeLabel.ROLE_PLAYER);
        edge.property(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID, this.type().labelId().getValue());
        edge.property(Schema.EdgeProperty.ROLE_LABEL_ID, role.labelId().getValue());
        Casting casting = Casting.create(edge, owner, role, toThing, conceptManager);
        conceptObserver.rolePlayerCreated(casting);
    }

    /**
     * Castings are retrieved from the perspective of the Relation
     *
     * @param roles The Role which the Things are playing
     * @return The Casting which unify a Role and Thing with this Relation
     */
    public Stream<Casting> castingsRelation(Role... roles) {
        Set<Role> roleSet = new HashSet<>(Arrays.asList(roles));
        if (roleSet.isEmpty()) {
            return vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.ROLE_PLAYER)
                    .map(edge -> Casting.withRelation(edge, owner, conceptManager));
        }

        //Traversal is used so we can potentially optimise on the index
        Set<Integer> roleTypesIds = roleSet.stream().map(r -> r.labelId().getValue()).collect(Collectors.toSet());

        Stream<EdgeElement> castingsEdges = vertex().roleCastingsEdges(type(), roleTypesIds);
        return castingsEdges.map(edge -> Casting.withRelation(edge, owner, conceptManager));
    }

    @Override
    public String innerToString() {
        StringBuilder description = new StringBuilder();
        description.append("ID [").append(id()).append("] Type [").append(type().label()).append("] Roles and Role Players: \n");
        for (Map.Entry<Role, Set<Thing>> entry : allRolePlayers().entrySet()) {
            if (entry.getValue().isEmpty()) {
                description.append("    Role [").append(entry.getKey().label()).append("] not played by any instance \n");
            } else {
                StringBuilder instancesString = new StringBuilder();
                for (Thing thing : entry.getValue()) {
                    instancesString.append(thing.id()).append(",");
                }
                description.append("    Role [").append(entry.getKey().label()).append("] played by [").
                        append(instancesString.toString()).append("] \n");
            }
        }
        return description.toString();
    }

    /**
     * Sets the owner of this structure to a specific RelationImpl.
     * This is so that the internal structure can use the Relation reference;
     *
     * @param relation the owner of this RelationReified
     */
    public void owner(RelationImpl relation) {
        owner = relation;
    }

    @Override
    public RelationReified reify() {
        return this;
    }

    @Override
    public boolean isReified() {
        return true;
    }

    @Override
    public Stream<Thing> getDependentConcepts() {
        return owner != null? owner.getDependentConcepts() : Stream.empty();
    }
}
