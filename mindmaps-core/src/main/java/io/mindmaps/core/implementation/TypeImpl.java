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
import io.mindmaps.core.model.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

class TypeImpl<T extends Type, V extends Concept> extends ConceptImpl<T, Type, String> implements Type {
    TypeImpl(Vertex v, MindmapsTransactionImpl mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    @Override
    public Collection<RoleType> playsRoles() {
        Set<RoleType> rolesPlayed = new HashSet<>();
        Iterator<Edge> edges = getVertex().edges(Direction.OUT, DataType.EdgeLabel.PLAYS_ROLE.getLabel());

        edges.forEachRemaining(edge -> {
            RoleTypeImpl roleType = getMindmapsTransaction().getElementFactory().buildRoleType(edge.inVertex());
            roleType.getAkoHierarchySubSet().forEach(role -> rolesPlayed.add(getMindmapsTransaction().getElementFactory().buildRoleType(role)));
        });

        return rolesPlayed;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T superType() {
        Concept concept = getOutgoingNeighbour(DataType.EdgeLabel.AKO);
        if(concept == null)
            return null;
        else
            return (T) concept;
    }

    @Override
    public void innerDelete(){
        Collection<? extends Concept> subSet = subTypes();
        Collection<? extends Concept> instanceSet = instances();
        subSet.remove(this);

        if(subSet.isEmpty() && instanceSet.isEmpty()){
            deleteNode();
        } else {
            throw new ConceptException(ErrorMessage.CANNOT_DELETE.getMessage(toString()));
        }
    }

    public Set<Type> getAkoHierarchySuperSet() {
        Set<Type> superSet= new HashSet<>();
        superSet.add(this);
        TypeImpl akoParent = getParentAko();

        while(akoParent != null){
            if(superSet.contains(akoParent))
                throw new ConceptException(ErrorMessage.LOOP_DETECTED.getMessage(toString(), DataType.EdgeLabel.AKO.getLabel()));
            else
                superSet.add(akoParent);
            akoParent = akoParent.getParentAko();
        }

        return superSet;
    }

    public Set<T> getAkoHierarchySubSet() {
        return nextAkoLevel(this);
    }

    @SuppressWarnings("unchecked")
    private Set<T> nextAkoLevel(TypeImpl<?, ?> root){
        Set<T> results = new HashSet<>();
        results.add((T) root);

        Collection<TypeImpl> children = root.getSubConceptTypes();
        for(TypeImpl child: children){
            results.addAll(nextAkoLevel(child));
        }

        return results;
    }

    @Override
    public Collection<T> subTypes(){
        return getAkoHierarchySubSet();
    }
    private Collection<TypeImpl> getSubConceptTypes(){
        Collection<TypeImpl> subSet = new HashSet<>();
        getIncomingNeighbours(DataType.EdgeLabel.AKO).forEach(concept -> {
            subSet.add(getMindmapsTransaction().getElementFactory().buildSpecificConceptType(concept));
        });
        return subSet;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<V> instances() {
        Set<V> instances = new HashSet<>();

        //noinspection unchecked
        GraphTraversal<Vertex, Vertex> traversal = getMindmapsTransaction().getTinkerPopGraph().traversal().V()
                .has(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER.name(), getId())
                .union(__.identity(), __.repeat(__.in(DataType.EdgeLabel.AKO.getLabel())).emit()).unfold()
                .in(DataType.EdgeLabel.ISA.getLabel())
                .union(__.identity(), __.repeat(__.in(DataType.EdgeLabel.AKO.getLabel())).emit()).unfold();

        traversal.forEachRemaining(vertex -> {
            ConceptImpl concept = getMindmapsTransaction().getElementFactory().buildUnknownConcept(vertex);
            if(!DataType.BaseType.CASTING.name().equals(concept.getBaseType())){
                instances.add((V) concept);
            }
        });

        return instances;
    }

    @Override
    public Boolean isAbstract() {
        Object object = getProperty(DataType.ConceptProperty.IS_ABSTRACT);
        return object != null && Boolean.parseBoolean(object.toString());
    }

    @Override
    public Collection<Rule> getRulesOfHypothesis() {
        Set<Rule> rules = new HashSet<>();
        getIncomingNeighbours(DataType.EdgeLabel.HYPOTHESIS).forEach(concept -> {
            rules.add(getMindmapsTransaction().getElementFactory().buildRule(concept));
        });
        return rules;
    }

    @Override
    public Collection<Rule> getRulesOfConclusion() {
        Set<Rule> rules = new HashSet<>();
        getIncomingNeighbours(DataType.EdgeLabel.CONCLUSION).forEach(concept -> {
            rules.add(getMindmapsTransaction().getElementFactory().buildRule(concept));
        });
        return rules;
    }

    public T superType(T type) {
        //Track any existing data if there is some
        Type currentSuperType = superType();
        if(currentSuperType != null){
            currentSuperType.instances().forEach(concept -> {
                if(concept.isInstance()){
                    ((InstanceImpl<?, ?, ?>) concept).castings().forEach(
                            instance -> mindmapsTransaction.getTransaction().putConcept(instance));
                }
            });
        }

        deleteEdges(Direction.OUT, DataType.EdgeLabel.AKO);
        deleteEdges(Direction.OUT, DataType.EdgeLabel.ISA);
        putEdge(getMindmapsTransaction().getElementFactory().buildSpecificConceptType(type), DataType.EdgeLabel.AKO);
        return getThis();
    }

    @Override
    public T playsRole(RoleType roleType) {
        putEdge(getMindmapsTransaction().getElementFactory().buildRoleType(roleType), DataType.EdgeLabel.PLAYS_ROLE);
        return getThis();
    }

    @Override
    public T deletePlaysRole(RoleType roleType) {
        deleteEdgeTo(DataType.EdgeLabel.PLAYS_ROLE, getMindmapsTransaction().getElementFactory().buildRoleType(roleType));

        //Add castings to tracking to make sure they can still be played.
        instances().forEach(concept -> {
            if (concept.isInstance()) {
                ((InstanceImpl<?, ?, ?>) concept).castings().forEach(casting -> mindmapsTransaction.getTransaction().putConcept(casting));
            }
        });

        return getThis();
    }

    @Override
    public String toString(){
        String message = super.toString();
        message = message + " - Abstract [" + isAbstract() + "] ";
        return message;
    }

    @Override
    public T setAbstract(Boolean isAbstract) {
        setProperty(DataType.ConceptProperty.IS_ABSTRACT, isAbstract);
        if(isAbstract)
            mindmapsTransaction.getTransaction().putConcept(this);
        return getThis();
    }
}
