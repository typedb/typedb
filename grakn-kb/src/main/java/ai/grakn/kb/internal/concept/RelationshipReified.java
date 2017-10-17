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

package ai.grakn.kb.internal.concept;

import ai.grakn.concept.Relationship;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraknTxOperationException;
import ai.grakn.kb.internal.cache.Cache;
import ai.grakn.kb.internal.cache.Cacheable;
import ai.grakn.kb.internal.structure.Casting;
import ai.grakn.kb.internal.structure.EdgeElement;
import ai.grakn.kb.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 *     Encapsulates The {@link Relationship} as a {@link VertexElement}
 * </p>
 *
 * <p>
 *     This wraps up a {@link Relationship} as a {@link VertexElement}. It is used to represent any {@link Relationship} which
 *     has been reified.
 * </p>
 *
 * @author fppt
 *
 */
public class RelationshipReified extends ThingImpl<Relationship, RelationshipType> implements RelationshipStructure {
    /**
     * Set of {@link Casting}s which are loaded into memory. This should only be used when adding new roleplayers.
     * This is because validation requires iterating over all castings anyway.
     */
    private Cache<Set<Casting>> allCastings = new Cache(Cacheable.set(), () -> lazyCastings(Collections.EMPTY_SET).collect(Collectors.toSet()));
    @Nullable private RelationshipImpl owner;

    private RelationshipReified(VertexElement vertexElement) {
        super(vertexElement);
    }

    private RelationshipReified(VertexElement vertexElement, RelationshipType type) {
        super(vertexElement, type);
    }

    public static RelationshipReified get(VertexElement vertexElement){
        return new RelationshipReified(vertexElement);
    }

    public static RelationshipReified create(VertexElement vertexElement, RelationshipType type){
        return new RelationshipReified(vertexElement, type);
    }

    @Override
    public Map<Role, Set<Thing>> allRolePlayers() {
        HashMap<Role, Set<Thing>> roleMap = new HashMap<>();

        //We add the role types explicitly so we can return them when there are no roleplayers
        type().relates().forEach(roleType -> roleMap.put(roleType, new HashSet<>()));
        //All castings are used here because we need to iterate over all of them anyway
        allCastings.get().forEach(rp -> roleMap.computeIfAbsent(rp.getRole(), (k) -> new HashSet<>()).add(rp.getRolePlayer()));

        return roleMap;
    }

    @Override
    public void delete(){
        super.delete();
        allCastings.clear();
    }

    @Override
    public Stream<Thing> rolePlayers(Role... roles) {
        return castingsRelation(roles).map(Casting::getRolePlayer);
    }

    //TODO: This could probably become more efficient in certain use cases
    void removeRolePlayer(Role role, Thing thing) {
        Set<Casting> castings = allCastings.get();
        Iterator<Casting> iterator = castings.iterator();

        while(iterator.hasNext()){
            Casting casting = iterator.next();
            if(casting.getRole().equals(role) && casting.getRolePlayer().equals(thing)){
                casting.delete();
                iterator.remove();
                break;
            }
        }
    }

    public void addRolePlayer(Role role, Thing thing) {
        Objects.requireNonNull(role);
        Objects.requireNonNull(thing);

        if(Schema.MetaSchema.isMetaLabel(role.getLabel())) throw GraknTxOperationException.metaTypeImmutable(role.getLabel());

        //Do the actual put of the role and role player
        addRolePlayerEdge(role, thing);
    }

    /**
     * If the edge does not exist then it adds a {@link Schema.EdgeLabel#ROLE_PLAYER} edge from
     * this {@link Relationship} to a target {@link Thing} which is playing some {@link Role}.
     *
     * If the edge does exist nothing is done.
     *
     * @param role The {@link Role} being played by the {@link Thing} in this {@link Relationship}
     * @param toThing The {@link Thing} playing a {@link Role} in this {@link Relationship}
     */
    public void addRolePlayerEdge(Role role, Thing toThing) {
        EdgeElement edge = this.addEdge(ConceptVertex.from(toThing), Schema.EdgeLabel.ROLE_PLAYER);
        edge.property(Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID, this.type().getLabelId().getValue());
        edge.property(Schema.EdgeProperty.ROLE_LABEL_ID, role.getLabelId().getValue());
        Casting casting = Casting.create(edge, owner, role, toThing);
        vertex().tx().txCache().trackForValidation(casting);

        //Cache the new casting
        allCastings.get().add(casting);
    }

    /**
     * Castings are retrieved from the perspective of the {@link Relationship}
     *
     * @param roles The {@link Role} which the {@link Thing}s are playing
     * @return The {@link Casting} which unify a {@link Role} and {@link Thing} with this {@link Relationship}
     */
    public Stream<Casting> castingsRelation(Role... roles){
        Set<Role> roleSet = new HashSet<>(Arrays.asList(roles));

        if(allCastings.isPresent()){
            Stream<Casting> castings = allCastings.get().stream();
            if(roleSet.isEmpty()) return castings;
            return allCastings.get().stream().filter(casting -> roleSet.contains(casting.getRole()));
        } else {
            return lazyCastings(roleSet);
        }
    }

    /**
     * Used the {@link #allCastings} has not been loaded. This should only be used when reading.
     * If writes are involved, this should not be used.
     *
     * @param roles The {@link Role} which the {@link Thing}s are playing
     * @return The {@link Casting} which unify a {@link Role} and {@link Thing} with this {@link Relationship}
     */
    private Stream<Casting> lazyCastings(Set<Role> roles){
        if(roles.isEmpty()){
            return vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.ROLE_PLAYER).
                    map(edge -> Casting.withRelationship(edge, owner));
        }

        //Traversal is used so we can potentially optimise on the index
        Set<Integer> roleTypesIds = roles.stream().map(r -> r.getLabelId().getValue()).collect(Collectors.toSet());
        return vertex().tx().getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), getId().getValue()).
                outE(Schema.EdgeLabel.ROLE_PLAYER.getLabel()).
                has(Schema.EdgeProperty.RELATIONSHIP_TYPE_LABEL_ID.name(), type().getLabelId().getValue()).
                has(Schema.EdgeProperty.ROLE_LABEL_ID.name(), P.within(roleTypesIds)).
                toStream().
                map(edge -> vertex().tx().factory().buildEdgeElement(edge)).
                map(edge -> Casting.withRelationship(edge, owner));
    }

    @Override
    public String innerToString(){
        StringBuilder description = new StringBuilder();
        description.append("ID [").append(getId()).append("] Type [").append(type().getLabel()).append("] Roles and Role Players: \n");
        for (Map.Entry<Role, Set<Thing>> entry : allRolePlayers().entrySet()) {
            if(entry.getValue().isEmpty()){
                description.append("    Role [").append(entry.getKey().getLabel()).append("] not played by any instance \n");
            } else {
                StringBuilder instancesString = new StringBuilder();
                for (Thing thing : entry.getValue()) {
                    instancesString.append(thing.getId()).append(",");
                }
                description.append("    Role [").append(entry.getKey().getLabel()).append("] played by [").
                        append(instancesString.toString()).append("] \n");
            }
        }
        return description.toString();
    }

    /**
     * Sets the owner of this structure to a specific {@link RelationshipImpl}.
     * This is so that the internal structure can use the {@link Relationship} reference;
     *
     * @param relationship the owner of this {@link RelationshipReified}
     */
    public void owner(RelationshipImpl relationship) {
        owner = relationship;
    }

    @Override
    public RelationshipReified reify() {
        return this;
    }

    @Override
    public boolean isReified() {
        return true;
    }
}
