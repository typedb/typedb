package io.mindmaps.core.implementation;

import io.mindmaps.core.model.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class RoleTypeImpl extends TypeImpl<RoleType, Instance> implements RoleType{
    RoleTypeImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    @Override
    public RelationType relationType() {
        Concept concept = getIncomingNeighbour(DataType.EdgeLabel.HAS_ROLE);

        if(concept == null){
            return null;
        } else {
            return getMindmapsGraph().getElementFactory().buildRelationType(concept);
        }
    }

    @Override
    public Collection<Type> playedByTypes() {
        Collection<Type> types = new HashSet<>();
        getIncomingNeighbours(DataType.EdgeLabel.PLAYS_ROLE).forEach(c -> types.add(getMindmapsGraph().getElementFactory().buildSpecificConceptType(c)));
        return types;
    }

    @Override
    public Collection<Instance> instances(){
        Set<Instance> instances = new HashSet<>();
        getIncomingNeighbours(DataType.EdgeLabel.ISA).forEach(concept -> {
            CastingImpl casting = (CastingImpl) concept;
            instances.add(casting.getRolePlayer());
        });
        return instances;
    }

    public Set<CastingImpl> castings(){
        Set<CastingImpl> castings = new HashSet<>();
        getIncomingNeighbours(DataType.EdgeLabel.ISA).forEach(concept -> {
            ((CastingImpl) concept).getRelations().forEach(relation -> mindmapsGraph.getTransaction().putConcept(relation));
        });
        return castings;
    }
}
