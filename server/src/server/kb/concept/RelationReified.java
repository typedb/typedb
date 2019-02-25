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

import grakn.core.concept.thing.Relation;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Role;
import grakn.core.server.exception.TransactionException;
import grakn.core.server.kb.Schema;
import grakn.core.server.kb.structure.Casting;
import grakn.core.server.kb.structure.EdgeElement;
import grakn.core.server.kb.structure.VertexElement;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

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

    private RelationReified(VertexElement vertexElement) {
        super(vertexElement);
    }

    private RelationReified(VertexElement vertexElement, RelationType type) {
        super(vertexElement, type);
    }

    public static RelationReified get(VertexElement vertexElement) {
        return new RelationReified(vertexElement);
    }

    public static RelationReified create(VertexElement vertexElement, RelationType type) {
        return new RelationReified(vertexElement, type);
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
                    vertex().tx().cache().remove(casting);
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
        GraphTraversal<Vertex, Edge> traversal = vertex().tx().getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), this.id().getValue()).
                outE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                has(Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID.name(), this.type().labelId().getValue()).
                has(Schema.EdgeProperty.ROLE_LABEL_ID.name(), role.labelId().getValue()).
                as("edge").
                inV().
                has(Schema.VertexProperty.ID.name(), toThing.id()).
                select("edge");

        if (traversal.hasNext()) {
            return;
        }

        //Role player edge does not exist create a new one
        EdgeElement edge = this.addEdge(ConceptVertex.from(toThing), Schema.EdgeLabel.ROLE_PLAYER);
        edge.property(Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID, this.type().labelId().getValue());
        edge.property(Schema.EdgeProperty.ROLE_LABEL_ID, role.labelId().getValue());
        Casting casting = Casting.create(edge, owner, role, toThing);
        vertex().tx().cache().trackForValidation(casting);
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
            return vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.ROLE_PLAYER).
                    map(edge -> Casting.withRelationship(edge, owner));
        }

        //Traversal is used so we can potentially optimise on the index
        Set<Integer> roleTypesIds = roleSet.stream().map(r -> r.labelId().getValue()).collect(Collectors.toSet());
        return vertex().tx().getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), id().getValue()).
                outE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                has(Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID.name(), type().labelId().getValue()).
                has(Schema.EdgeProperty.ROLE_LABEL_ID.name(), P.within(roleTypesIds)).
                toStream().
                map(edge -> vertex().tx().factory().buildEdgeElement(edge)).
                map(edge -> Casting.withRelationship(edge, owner));
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
     * @param relationship the owner of this RelationReified
     */
    public void owner(RelationImpl relationship) {
        owner = relationship;
    }

    @Override
    public RelationReified reify() {
        return this;
    }

    @Override
    public boolean isReified() {
        return true;
    }

}
