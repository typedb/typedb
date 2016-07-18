package io.mindmaps.core.implementation;

import io.mindmaps.core.model.Relation;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.RoleType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class RelationTypeImpl extends TypeImpl<RelationType, Relation> implements RelationType {
    RelationTypeImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    @Override
    public Collection<RoleType> hasRoles() {
        Set<RoleType> roleTypes = new HashSet<>();
        getOutgoingNeighbours(DataType.EdgeLabel.HAS_ROLE).forEach(role -> roleTypes.add(getMindmapsGraph().getElementFactory().buildRoleType(role)));
        return roleTypes;
    }

    @Override
    public RelationType hasRole(RoleType roleType) {
        putEdge(getMindmapsGraph().getElementFactory().buildRoleType(roleType), DataType.EdgeLabel.HAS_ROLE);
        return this;
    }

    @Override
    public RelationType deleteHasRole(RoleType roleType) {
        deleteEdgeTo(DataType.EdgeLabel.HAS_ROLE, getMindmapsGraph().getElementFactory().buildRoleType(roleType));
        //Add castings of roleType to make sure relations are still valid
        ((RoleTypeImpl) roleType).castings().forEach(casting -> mindmapsGraph.getTransaction().putConcept(casting));
        return this;
    }
}
