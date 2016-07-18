package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.NoEdgeException;
import io.mindmaps.core.model.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

class CastingImpl extends ConceptImpl {

    CastingImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    public RoleTypeImpl getRole() {
        Concept concept = getParentIsa();
        if(concept != null)
            return getMindmapsGraph().getElementFactory().buildRoleType(concept);
        else
            throw new NoEdgeException(toString(), DataType.BaseType.ROLE_TYPE.name());
    }

    public InstanceImpl getRolePlayer() {
        Concept concept = getOutgoingNeighbour(DataType.EdgeLabel.ROLE_PLAYER);
        if(concept != null)
            return getMindmapsGraph().getElementFactory().buildSpecificInstance(concept);
        else
            return null;
    }

    public CastingImpl setHash(RoleTypeImpl role, InstanceImpl rolePlayer){
        String hash;
        if(getMindmapsGraph().isBatchLoadingEnabled())
            hash = "CastingBaseId_" + this.getBaseIdentifier() + UUID.randomUUID().toString();
        else
            hash = generateNewHash(role, rolePlayer);
        setUniqueProperty(DataType.ConceptPropertyUnique.INDEX, hash);
        return this;
    }

    public static String generateNewHash(RoleTypeImpl role, InstanceImpl rolePlayer){
        return "Casting-Role-" + role.getId() + "-RolePlayer-" + rolePlayer.getId();
    }

    public Set<RelationImpl> getRelations() {
        ConceptImpl<?, ?, ?> thisRef = this;
        Set<RelationImpl> relations = new HashSet<>();
        Set<ConceptImpl> concepts = thisRef.getIncomingNeighbours(DataType.EdgeLabel.CASTING);

        if(concepts.size() > 0){
            relations.addAll(concepts.stream().map(getMindmapsGraph().getElementFactory()::buildRelation).collect(Collectors.toList()));
        } else {
            throw new NoEdgeException(toString(), DataType.BaseType.RELATION.name());
        }

        return relations;
    }
}
