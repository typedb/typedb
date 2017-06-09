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
import ai.grakn.exception.GraphOperationException;
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
 *     Encapsulates relationships between {@link Instance}
 * </p>
 *
 * <p>
 *     A relation which is an instance of a {@link RelationType} defines how instances may relate to one another.
 * </p>
 *
 * @author fppt
 *
 */
class RelationImpl extends InstanceImpl<Relation, RelationType> implements Relation {
    RelationImpl(VertexElement vertexElement) {
        super(vertexElement);
    }

    RelationImpl(VertexElement vertexElement, RelationType type) {
        super(vertexElement, type);
    }

    /**
     * Sets the internal hash in order to perform a faster lookup
     */
    void setHash(){
        setUniqueProperty(Schema.ConceptProperty.INDEX, generateNewHash(type(), allRolePlayers()));
    }

    /**
     * Castings are retrieved from the perspective of the {@link Relation}
     *
     * @param roleTypes The role which the instances are playing
     * @return The {@link Casting} which unify a {@link RoleType} and {@link Instance} with this {@link Relation}
     */
    Stream<Casting> castingsRelation(RoleType... roleTypes){
        if(roleTypes.length == 0){
            return vertex().getEdgesOfType(Direction.OUT, Schema.EdgeLabel.SHORTCUT).
                    map(edge -> graph().getElementFactory().buildRolePlayer(edge));
        }

        //Traversal is used so we can potentially optimise on the index
        Set<Integer> roleTypesIds = Arrays.stream(roleTypes).map(r -> r.getTypeId().getValue()).collect(Collectors.toSet());
        return graph().getTinkerTraversal().
                has(Schema.ConceptProperty.ID.name(), getId().getValue()).
                outE(Schema.EdgeLabel.SHORTCUT.getLabel()).
                has(Schema.EdgeProperty.RELATION_TYPE_ID.name(), type().getTypeId().getValue()).
                has(Schema.EdgeProperty.ROLE_TYPE_ID.name(), P.within(roleTypesIds)).
                toStream().map(edge -> graph().getElementFactory().buildRolePlayer(edge));
    }

    /**
     *
     * @param relationType The type of this relation
     * @param roleMap The roles and their corresponding role players
     * @return A unique hash identifying this relation
     */
    static String generateNewHash(RelationType relationType, Map<RoleType, Set<Instance>> roleMap){
        SortedSet<RoleType> sortedRoleIds = new TreeSet<>(roleMap.keySet());
        StringBuilder hash = new StringBuilder();
        hash.append("RelationType_").append(relationType.getId().getValue().replace("_", "\\_")).append("_Relation");

        for(RoleType role: sortedRoleIds){
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
     * Retrieve a list of all Instances involved in the Relation, and the Role Types they play.
     * @see RoleType
     *
     * @return A list of all the role types and the instances playing them in this relation.
     */
    @Override
    public Map<RoleType, Set<Instance>> allRolePlayers(){
        HashMap<RoleType, Set<Instance>> roleMap = new HashMap<>();

        //We add the role types explicitly so we can return them when there are no roleplayers
        type().relates().forEach(roleType -> roleMap.put(roleType, new HashSet<>()));
        castingsRelation().forEach(rp -> roleMap.computeIfAbsent(rp.getRoleType(), (k) -> new HashSet<>()).add(rp.getInstance()));

        return roleMap;
    }

    @Override
    public Collection<Instance> rolePlayers(RoleType... roleTypes) {
        return castingsRelation(roleTypes).map(Casting::getInstance).collect(Collectors.toSet());
    }


    /**
     * Expands this Relation to include a new role player which is playing a specific role.
     * @param roleType The role of the new role player.
     * @param instance The new role player.
     * @return The Relation itself
     */
    @Override
    public Relation addRolePlayer(RoleType roleType, Instance instance) {
        Objects.requireNonNull(roleType);
        Objects.requireNonNull(instance);

        if(Schema.MetaSchema.isMetaLabel(roleType.getLabel())) throw GraphOperationException.metaTypeImmutable(roleType.getLabel());

        //Do the actual put of the role and role player
        return addNewRolePlayer(roleType, instance);
    }

    /**
     * Adds a new role player to this relation
     * @param roleType The role of the new role player.
     * @param instance The new role player.
     * @return The Relation itself
     */
    private Relation addNewRolePlayer(RoleType roleType, Instance instance){
        graph().putShortcutEdge((InstanceImpl) instance, this, (RoleTypeImpl) roleType);
        return this;
    }

    /**
     * When a relation is deleted this cleans up any solitary casting and resources.
     */
    void cleanUp() {
        boolean performDeletion = true;
        Collection<Instance> rolePlayers = rolePlayers();

        for(Instance instance : rolePlayers){
            if(instance != null && (instance.getId() != null )){
                performDeletion = false;
            }
        }

        if(performDeletion){
            delete();
        }
    }

    @Override
    public String innerToString(){
        StringBuilder description = new StringBuilder();
        description.append("ID [").append(getId()).append("] Type [").append(type().getLabel()).append("] Roles and Role Players: \n");
        for (Map.Entry<RoleType, Set<Instance>> entry : allRolePlayers().entrySet()) {
            if(entry.getValue().isEmpty()){
                description.append("    Role [").append(entry.getKey().getLabel()).append("] not played by any instance \n");
            } else {
                StringBuilder instancesString = new StringBuilder();
                for (Instance instance : entry.getValue()) {
                    instancesString.append(instance.getId()).append(",");
                }
                description.append("    Role [").append(entry.getKey().getLabel()).append("] played by [").
                        append(instancesString.toString()).append("] \n");
            }
        }
        return description.toString();
    }
}
