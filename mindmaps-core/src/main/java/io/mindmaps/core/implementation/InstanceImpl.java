package io.mindmaps.core.implementation;

import io.mindmaps.core.exceptions.ConceptException;
import io.mindmaps.core.model.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

abstract class InstanceImpl<T extends Instance, V extends Type, D> extends ConceptImpl<T, V, D> implements Instance {
    InstanceImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    @Override
    public void innerDelete() throws ConceptException {
        InstanceImpl<?, ?, ?> parent = this;
        Set<CastingImpl> castings = parent.castings();
        deleteNode();
        for(CastingImpl casting: castings){
            Set<RelationImpl> relations = casting.getRelations();
            getMindmapsGraph().getTransaction().putConcept(casting);

            for(RelationImpl relation : relations) {
                getMindmapsGraph().getTransaction().putConcept(relation);
                relation.cleanUp();
            }

            casting.deleteNode();
        }
    }

    public String getIndex(){
        return getProperty(DataType.ConceptPropertyUnique.INDEX);
    }

    public Collection<Resource<?>> resources() {
        Set<Resource<?>> resources = new HashSet<>();
        this.getOutgoingNeighbours(DataType.EdgeLabel.SHORTCUT).forEach(concept -> {
            if(concept.isResource()) {
                Resource<?> resource = concept.asResource();
                resources.add(resource);
            }
        });
        return resources;
    }

    public Set<CastingImpl> castings(){
        Set<CastingImpl> castings = new HashSet<>();
        getIncomingNeighbours(DataType.EdgeLabel.ROLE_PLAYER).forEach(casting -> {
            castings.add((CastingImpl) casting);
        });
        return castings;
    }

    @Override
    public Collection<Relation> relations(RoleType... roleTypes) {
        Set<Relation> relations = new HashSet<>();
        Set<String> roleTypeItemIdentifier = new HashSet<>();
        for (RoleType roleType : roleTypes) {
            roleTypeItemIdentifier.add(roleType.getId());
        }

        InstanceImpl<?, ?, ?> parent = this;

        parent.castings().forEach(c -> {
            CastingImpl casting = getMindmapsGraph().getElementFactory().buildCasting(c);
            if (roleTypeItemIdentifier.size() != 0) {
                if (roleTypeItemIdentifier.contains(casting.getType()))
                    relations.addAll(casting.getRelations());
            } else {
                relations.addAll(casting.getRelations());
            }
        });

        return relations;
    }

    @Override
    public Collection<RoleType> playsRoles() {
        Set<RoleType> roleTypes = new HashSet<>();
        ConceptImpl<?, ?, ?> parent = this;
        parent.getIncomingNeighbours(DataType.EdgeLabel.ROLE_PLAYER).forEach(c -> {
            roleTypes.add(getMindmapsGraph().getElementFactory().buildCasting(c).getRole());
        });
        return roleTypes;
    }
}
