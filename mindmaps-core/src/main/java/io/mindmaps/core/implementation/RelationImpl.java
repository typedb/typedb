/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.core.implementation;

import io.mindmaps.constants.DataType;
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.implementation.exception.ConceptException;
import io.mindmaps.core.model.Instance;
import io.mindmaps.core.model.Relation;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

/**
 * A relation represents and instance of a relation type which model how different entities relate to one another.
 */
class RelationImpl extends InstanceImpl<Relation, RelationType> implements Relation {
    RelationImpl(Vertex v, AbstractMindmapsGraph mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    /**
     *
     * @return All the castings this relation is connected with
     */
    public Set<CastingImpl> getMappingCasting() {
        Set<CastingImpl> castings = new HashSet<>();
        getOutgoingNeighbours(DataType.EdgeLabel.CASTING).forEach(casting -> castings.add(getMindmapsGraph().getElementFactory().buildCasting(casting)));
        return castings;
    }

    /**
     * Sets the internal hash in order to perform a faster lookup
     * @param roleMap The roles and their corresponding role players
     */
    public void setHash(Map<RoleType, Instance> roleMap){
        if(roleMap == null || roleMap.isEmpty())
            setUniqueProperty(DataType.ConceptPropertyUnique.INDEX, "RelationBaseId_" + getBaseIdentifier() + UUID.randomUUID().toString());
        else
            setUniqueProperty(DataType.ConceptPropertyUnique.INDEX, generateNewHash(type(), roleMap));
    }

    /**
     *
     * @param relationType The type of this relation
     * @param roleMap The roles and their corresponding role players
     * @return A unique hash identifying this relation
     */
    public static String generateNewHash(RelationType relationType, Map<RoleType, Instance> roleMap){
        SortedSet<RoleType> sortedRoleIds = new TreeSet<>(roleMap.keySet());
        String hash = "RelationType_" + relationType.getId().replace("_", "\\_") + "_Relation";

        for(RoleType role: sortedRoleIds){
            hash = hash + "_" + role.getId().replace("_", "\\_") ;
            Instance instance = roleMap.get(role);
            if(instance != null){
                hash = hash + "_" + instance.getId().replace("_", "\\_") ;
            }
        }
        return hash;
    }

    /**
     *
     * @return A list of all the Instances involved in the relationships and the Role Types which they play.
     */
    @Override
    public Map<RoleType, Instance> rolePlayers() {
        Set<CastingImpl> castings = getMappingCasting();
        HashMap<RoleType, Instance> roleMap = new HashMap<>();

        //Gets roles based on all roles of the relation type
        type().hasRoles().forEach(roleType -> roleMap.put(roleType, null));

        //Get roles based on availiable castings
        castings.forEach(casting -> roleMap.put(casting.getRole(), casting.getRolePlayer()));

        return roleMap;
    }

    /**
     *
     * @return A list of the Instances which scope this Relation
     */
    @Override
    public Set<Instance> scopes() {
        HashSet<Instance> scopes = new HashSet<>();
        getOutgoingNeighbours(DataType.EdgeLabel.HAS_SCOPE).forEach(concept -> scopes.add(getMindmapsGraph().getElementFactory().buildSpecificInstance(concept)));
        return scopes;
    }

    /**
     *
     * @param instance A new instance which can scope this Relation
     * @return The Relation itself
     */
    @Override
    public Relation scope(Instance instance) {
        putEdge(getMindmapsGraph().getElementFactory().buildEntity(instance), DataType.EdgeLabel.HAS_SCOPE);
        return this;
    }

    /**
     * Expands this Relation to include a new role player which is playing a specific role.
     * @param roleType The role of the new role player.
     * @param instance The new role player.
     * @return The Relation itself
     */
    @Override
    public Relation putRolePlayer(RoleType roleType, Instance instance) {
        if(roleType == null){
            throw new IllegalArgumentException(ErrorMessage.ROLE_IS_NULL.getMessage(instance));
        }

        if(mindmapsGraph.isBatchLoadingEnabled()) {
            return addNewRolePlayer(null, roleType, instance);
        } else {
            Map<RoleType, Instance> roleMap = rolePlayers();
            roleMap.put(roleType, instance);
            Relation otherRelation = mindmapsGraph.getRelation(type(), roleMap);

            if(otherRelation == null){
                return addNewRolePlayer(roleMap, roleType, instance);
            }

            if(!this.equals(otherRelation)){
                throw new ConceptException(ErrorMessage.RELATION_EXISTS.getMessage(otherRelation));
            } else {
                return this;
            }
        }
    }

    /**
     * Adds a new role player to this relation
     * @param roleType The role of the new role player.
     * @param instance The new role player.
     * @return The Relation itself
     */
    private Relation addNewRolePlayer(Map<RoleType, Instance> roleMap, RoleType roleType, Instance instance){
        if(instance != null)
            mindmapsGraph.putCasting((RoleTypeImpl) roleType, (InstanceImpl) instance, this);

        if(mindmapsGraph.isBatchLoadingEnabled()){
            setHash(null);
        } else {
            setHash(roleMap);
        }

        return this;
    }

    /**
     * @param scope A concept which is currently scoping this concept.
     * @return The Relation itself
     */
    @Override
    public Relation deleteScope(Instance scope) throws ConceptException {
        deleteEdgeTo(DataType.EdgeLabel.HAS_SCOPE, getMindmapsGraph().getElementFactory().buildEntity(scope));
        return this;
    }

    /**
     * When a relation is deleted this cleans up any solitary casting and resources.
     */
    public void cleanUp() {
        boolean performDeletion = true;
        Collection<Instance> rolePlayers = rolePlayers().values();

        // tracking
        rolePlayers.forEach(r -> {
            if(r != null)
                getMindmapsGraph().getConceptLog().putConcept(getMindmapsGraph().getElementFactory().buildSpecificInstance(r));
        });
        this.getMappingCasting().forEach(c -> getMindmapsGraph().getConceptLog().putConcept(c));

        for(Instance instance : rolePlayers){
            if(instance != null && (instance.getId() != null )){
                performDeletion = false;
            }
        }

        if(performDeletion){
            delete();
        }
    }

    /**
     * Deletes the concept as a Relation
     */
    @Override
    public void innerDelete() {
        scopes().forEach(this::deleteScope);
        Set<CastingImpl> castings = getMappingCasting();

        for (CastingImpl casting: castings) {
            InstanceImpl<?, ?> instance = casting.getRolePlayer();
            if(instance != null) {
                for (EdgeImpl edge : instance.getEdgesOfType(Direction.BOTH, DataType.EdgeLabel.SHORTCUT)) {
                    if(edge.getProperty(DataType.EdgeProperty.RELATION_ID).equals(getId())){
                        edge.delete();
                    }
                }
            }
        }

        super.innerDelete();
    }
}
