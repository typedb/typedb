/*
 * Copyright (C) 2020 Grakn Labs
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
 *
 */

package grakn.core.concept.impl;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Casting;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.concept.manager.ConceptNotificationChannel;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.concept.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Encapsulates relations between Thing
 * A relation which is an instance of a RelationType defines how instances may relate to one another.
 */
public class RelationImpl extends ThingImpl<Relation, RelationType> implements Relation, ConceptVertex {

    public RelationImpl(VertexElement vertexElement, ConceptManager conceptManager, ConceptNotificationChannel conceptNotificationChannel){
        super(vertexElement, conceptManager, conceptNotificationChannel);
    }

    public static RelationImpl from(Relation relation) {
        return (RelationImpl) relation;
    }

    /**
     * Retrieve a list of all Thing involved in the Relation, and the Role they play.
     *
     * @return A list of all the Roles and the Things playing them in this Relation.
     * see Role
     */
    @Override
    public Map<Role, List<Thing>> rolePlayersMap() {
        HashMap<Role, List<Thing>> roleMap = new HashMap<>();

        //We add the role types explicitly so we can return them when there are no roleplayers
//        type().roles().forEach(roleType -> roleMap.put(roleType, new ArrayList<>()));
        //All castings are used here because we need to iterate over all of them anyway
        castingsRelation().forEach(rp -> roleMap.computeIfAbsent(rp.getRole(), (k) -> new ArrayList<>()).add(rp.getRolePlayer()));

        return roleMap;
    }

    /**
     * Expands this Relation to include a new role player which is playing a specific Role.
     *
     * @param role   The role of the new role player.
     * @param player The new role player.
     * @return The Relation itself
     */
    @Override
    public Relation assign(Role role, Thing player) {
        addRolePlayer(role, player);
        return this;
    }

    @Override
    public void unassign(Role role, Thing player) {
        removeRolePlayerIfPresent(role, player);
        // may need to clean up relation
        cleanUp();
    }

    /**
     * Remove this relation if there are no more role player present
     */
    void cleanUp() {
        boolean performDeletion = !rolePlayers().findAny().isPresent();
        if (performDeletion) delete();
    }

    @Override
    public Stream<Thing> rolePlayers(Role... roles) {
        return castingsRelation(roles).map(Casting::getRolePlayer);
    }


    /**
     * Remove a single single instance of specific role player playing a given role in this relation
     * We could have duplicates, so we only operate on a single casting that is found
     */
    private void removeRolePlayerIfPresent(Role role, Thing thing) {
        castingsRelation(role)
                .filter(casting -> casting.getRole().equals(role) && casting.getRolePlayer().equals(thing))
                .findAny()
                .ifPresent(casting -> {
                    casting.delete();
                    conceptNotificationChannel.castingDeleted(casting);
                });
    }
    private void addRolePlayer(Role role, Thing thing) {
        Objects.requireNonNull(role);
        Objects.requireNonNull(thing);

        if (Schema.MetaSchema.isMetaLabel(role.label())) throw GraknConceptException.metaTypeImmutable(role.label());

        //Do the actual put of the role and role player
        EdgeElement edge = this.addEdge(ConceptVertex.from(thing), Schema.EdgeLabel.ROLE_PLAYER);
        edge.property(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID, this.type().labelId().getValue());
        edge.property(Schema.EdgeProperty.ROLE_LABEL_ID, role.labelId().getValue());
        Casting casting = CastingImpl.create(edge, this, role, thing, conceptManager);
        conceptNotificationChannel.rolePlayerCreated(casting);
    }

    /**
     * Castings are retrieved from the perspective of the Relation
     *
     * @param roles The Role which the Things are playing
     * @return The Casting which unify a Role and Thing with this Relation
     */
    @Override
    public Stream<Casting> castingsRelation(Role... roles) {
        Set<Role> roleSet = new HashSet<>(Arrays.asList(roles));
        if (roleSet.isEmpty()) {
            return vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.ROLE_PLAYER)
                    .map(edge -> CastingImpl.withRelation(edge, this, conceptManager));
        }

        //Traversal is used so we can potentially optimise on the index
        Set<Integer> roleTypesIds = roleSet.stream().map(r -> r.labelId().getValue()).collect(Collectors.toSet());

        Stream<EdgeElement> castingsEdges = vertex().roleCastingsEdges(type().labelId().getValue(), roleTypesIds);
        return castingsEdges.map(edge -> CastingImpl.withRelation(edge, this, conceptManager));
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        return id().equals(((RelationImpl) object).id());
    }

    @Override
    public int hashCode() {
        return id().hashCode();
    }

    @Override
    public String innerToString() {
        StringBuilder description = new StringBuilder();
        description.append("ID [").append(id()).append("] Type [").append(type().label()).append("] Roles and Role Players: \n");
        for (Map.Entry<Role, List<Thing>> entry : rolePlayersMap().entrySet()) {
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
}
