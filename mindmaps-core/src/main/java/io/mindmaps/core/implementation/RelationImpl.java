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

import io.mindmaps.core.exceptions.ConceptException;
import io.mindmaps.core.exceptions.ErrorMessage;
import io.mindmaps.core.model.Instance;
import io.mindmaps.core.model.Relation;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

class RelationImpl extends InstanceImpl<Relation, RelationType, String> implements Relation {
    RelationImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    public Set<CastingImpl> getMappingCasting() {
        Set<CastingImpl> castings = new HashSet<>();
        getOutgoingNeighbours(DataType.EdgeLabel.CASTING).forEach(casting -> castings.add(getMindmapsTransaction().getElementFactory().buildCasting(casting)));
        return castings;
    }

    public void setHash(Map<RoleType, Instance> roleMap){
        if(roleMap == null || roleMap.isEmpty())
            setUniqueProperty(DataType.ConceptPropertyUnique.INDEX, "RelationBaseId_" + getBaseIdentifier() + UUID.randomUUID().toString());
        else
            setUniqueProperty(DataType.ConceptPropertyUnique.INDEX, generateNewHash(type(), roleMap));
    }

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

    @Override
    public Map<RoleType, Instance> rolePlayers() {
        Set<CastingImpl> castings = getMappingCasting();
        HashMap<RoleType, Instance> roleMap = new HashMap<>();

        //Gets roles based on all roles of the relation type
        type().hasRoles().forEach(roleType -> {
            roleMap.put(roleType, null);
        });

        //Get roles based on availiable castings
        castings.forEach(casting -> {
            roleMap.put(casting.getRole(), casting.getRolePlayer());
        });

        return roleMap;
    }

    @Override
    public Set<Instance> scopes() {
        HashSet<Instance> scopes = new HashSet<>();
        getOutgoingNeighbours(DataType.EdgeLabel.HAS_SCOPE).forEach(concept -> scopes.add(getMindmapsTransaction().getElementFactory().buildSpecificInstance(concept)));
        return scopes;
    }

    @Override
    public Relation scope(Instance instance) {
        putEdge(getMindmapsTransaction().getElementFactory().buildEntity(instance), DataType.EdgeLabel.HAS_SCOPE);
        return this;
    }

    @Override
    public Relation putRolePlayer(RoleType roleType, Instance instance) {
        if(roleType == null){
            throw new IllegalArgumentException(ErrorMessage.ROLE_IS_NULL.getMessage(instance));
        }

        if(mindmapsTransaction.isBatchLoadingEnabled()) {
            return addNewRolePlayer(null, roleType, instance);
        } else {
            Map<RoleType, Instance> roleMap = rolePlayers();
            roleMap.put(roleType, instance);
            Relation otherRelation = mindmapsTransaction.getRelation(type(), roleMap);

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
    private Relation addNewRolePlayer(Map<RoleType, Instance> roleMap, RoleType roleType, Instance instance){
        if(instance != null)
            mindmapsTransaction.putCasting((RoleTypeImpl) roleType, (InstanceImpl) instance, this);

        if(mindmapsTransaction.isBatchLoadingEnabled()){
            setHash(null);
        } else {
            setHash(roleMap);
        }

        return this;
    }


    @Override
    public Relation deleteScope(Instance scope) throws ConceptException {
        deleteEdgeTo(DataType.EdgeLabel.HAS_SCOPE, getMindmapsTransaction().getElementFactory().buildEntity(scope));
        return this;
    }

    public void cleanUp() throws ConceptException {
        boolean performDeletion = true;
        Collection<Instance> rolePlayers = rolePlayers().values();

        // tracking
        rolePlayers.forEach(r -> {
            if(r != null)
                getMindmapsTransaction().getTransaction().putConcept(getMindmapsTransaction().getElementFactory().buildSpecificInstance(r));
        });
        this.getMappingCasting().forEach(c -> getMindmapsTransaction().getTransaction().putConcept(c));

        for(Instance instance : rolePlayers){
            if(instance != null && (instance.getId() != null || instance.getSubject() != null)){
                performDeletion = false;
            }
        }

        if(performDeletion){
            delete();
        }
    }

    @Override
    public void innerDelete() throws ConceptException {
        scopes().forEach(this::deleteScope);
        Set<CastingImpl> castings = getMappingCasting();

        for (CastingImpl casting: castings) {
            InstanceImpl<?, ?, ?> instance = casting.getRolePlayer();
            if(instance != null) {
                for (EdgeImpl edge : instance.getEdgesOfType(Direction.BOTH, DataType.EdgeLabel.SHORTCUT)) {
                    if(edge.getEdgePropertyBaseAssertionId().equals(getBaseIdentifier())){
                        edge.delete();
                    }
                }
            }
            casting.deleteEdges(Direction.OUT, DataType.EdgeLabel.ROLE_PLAYER);
            casting.delete();
        }

        super.innerDelete();
    }
}
