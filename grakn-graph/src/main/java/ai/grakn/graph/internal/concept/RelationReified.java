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

import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.graph.internal.structure.Casting;
import ai.grakn.graph.internal.structure.VertexElement;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 *     Encapsulates The {@link Relation} as a {@link VertexElement}
 * </p>
 *
 * <p>
 *     This wraps up a {@link Relation} as a {@link VertexElement}. It is used to represent any {@link Relation} which
 *     has been reified.
 * </p>
 *
 * @author fppt
 *
 */
public class RelationReified extends ThingImpl<Relation, RelationType> implements RelationStructure {
    public RelationReified(VertexElement vertexElement) {
        super(vertexElement);
    }

    public RelationReified(VertexElement vertexElement, RelationType type) {
        super(vertexElement, type);
    }

    public Map<Role, Set<Thing>> allRolePlayers() {
        HashMap<Role, Set<Thing>> roleMap = new HashMap<>();

        //We add the role types explicitly so we can return them when there are no roleplayers
        type().relates().forEach(roleType -> roleMap.put(roleType, new HashSet<>()));
        castingsRelation().forEach(rp -> roleMap.computeIfAbsent(rp.getRoleType(), (k) -> new HashSet<>()).add(rp.getInstance()));

        return roleMap;
    }

    public Collection<Thing> rolePlayers(Role... roles) {
        return castingsRelation(roles).map(Casting::getInstance).collect(Collectors.toSet());
    }

    public void addRolePlayer(Role role, Thing thing) {
        Objects.requireNonNull(role);
        Objects.requireNonNull(thing);

        if(Schema.MetaSchema.isMetaLabel(role.getLabel())) throw GraphOperationException.metaTypeImmutable(role.getLabel());

        //Do the actual put of the role and role player
        vertex().graph().putShortcutEdge(thing, this, role);
    }

    /**
     * Sets the internal hash in order to perform a faster lookup
     */
    public void setHash(){
        vertex().propertyUnique(Schema.VertexProperty.INDEX, generateNewHash(type(), allRolePlayers()));
    }

    /**
     *
     * @param relationType The type of this relation
     * @param roleMap The roles and their corresponding role players
     * @return A unique hash identifying this relation
     */
    public static String generateNewHash(RelationType relationType, Map<Role, Set<Thing>> roleMap){
        SortedSet<Role> sortedRoleIds = new TreeSet<>(roleMap.keySet());
        StringBuilder hash = new StringBuilder();
        hash.append("RelationType_").append(relationType.getId().getValue().replace("_", "\\_")).append("_Relation");

        for(Role role: sortedRoleIds){
            hash.append("_").append(role.getId().getValue().replace("_", "\\_"));

            roleMap.get(role).forEach(instance -> {
                if(instance != null){
                    hash.append("_").append(instance.getId().getValue().replace("_", "\\_"));
                }
            });
        }
        return hash.toString();
    }

    /**
     * Castings are retrieved from the perspective of the {@link Relation}
     *
     * @param roles The role which the instances are playing
     * @return The {@link Casting} which unify a {@link Role} and {@link Thing} with this {@link Relation}
     */
    public Stream<Casting> castingsRelation(Role... roles){
        if(roles.length == 0){
            return vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHORTCUT).
                    map(edge -> vertex().graph().factory().buildCasting(edge));
        }

        //Traversal is used so we can potentially optimise on the index
        Set<Integer> roleTypesIds = Arrays.stream(roles).map(r -> r.getLabelId().getValue()).collect(Collectors.toSet());
        return vertex().graph().getTinkerTraversal().V().
                has(Schema.VertexProperty.ID.name(), getId().getValue()).
                outE(Schema.EdgeLabel.SHORTCUT.getLabel()).
                has(Schema.EdgeProperty.RELATION_TYPE_LABEL_ID.name(), type().getLabelId().getValue()).
                has(Schema.EdgeProperty.ROLE_LABEL_ID.name(), P.within(roleTypesIds)).
                toStream().map(edge -> vertex().graph().factory().buildCasting(edge));
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

    @Override
    public RelationReified reify() {
        return this;
    }

    @Override
    public boolean isReified() {
        return true;
    }
}
