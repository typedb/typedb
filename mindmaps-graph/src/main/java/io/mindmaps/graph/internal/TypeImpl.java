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

package io.mindmaps.graph.internal;

import io.mindmaps.util.Schema;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.exception.ConceptException;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Rule;
import io.mindmaps.concept.Type;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A Type represents any ontological element in the graph. For example Entity Types and Rule Types.
 * @param <T> The leaf interface of the object concept. For example an EntityType, Entity, RelationType etc . . .
 * @param <V> The type of the instances of this concept type.
 */
class TypeImpl<T extends Type, V extends Concept> extends ConceptImpl<T, Type> implements Type {
    TypeImpl(Vertex v, AbstractMindmapsGraph mindmapsGraph) {
        super(v, mindmapsGraph);
    }

    /**
     *
     * @return A list of all the roles this Type is allowed to play.
     */
    @Override
    public Collection<RoleType> playsRoles() {
        Set<RoleType> rolesPlayed = new HashSet<>();
        Iterator<Edge> edges = getVertex().edges(Direction.OUT, Schema.EdgeLabel.PLAYS_ROLE.getLabel());

        edges.forEachRemaining(edge -> {
            RoleTypeImpl roleType = getMindmapsGraph().getElementFactory().buildRoleType(edge.inVertex());
            roleType.subTypes().forEach(role -> rolesPlayed.add(getMindmapsGraph().getElementFactory().buildRoleType(role)));
        });

        return rolesPlayed;
    }

    /**
     *
     * @return This type's super type
     */
    @Override
    @SuppressWarnings("unchecked")
    public T superType() {
        Concept concept = getOutgoingNeighbour(Schema.EdgeLabel.AKO);
        if(concept == null)
            return null;
        else
            return (T) concept;
    }

    /**
     * Deletes the concept as a type
     */
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

    /**
     *
     * @return All outgoing ako parents including itself
     */
    public Set<Type> getAkoHierarchySuperSet() {
        Set<Type> superSet= new HashSet<>();
        superSet.add(this);
        TypeImpl akoParent = getParentAko();

        while(akoParent != null){
            if(superSet.contains(akoParent))
                throw new ConceptException(ErrorMessage.LOOP_DETECTED.getMessage(toString(), Schema.EdgeLabel.AKO.getLabel()));
            else
                superSet.add(akoParent);
            akoParent = akoParent.getParentAko();
        }

        return superSet;
    }

    /**
     *
     * @param root The current type to example
     * @return All the ako children of the root. Effectively calls  {@link TypeImpl#getSubConceptTypes()} recursively
     */
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

    /**
     *
     * @return All the subtypes of this concept including itself
     */
    @Override
    public Collection<T> subTypes(){
        return nextAkoLevel(this);
    }

    /**
     *
     * @return All of the concepts direct ako children spanning a single level.
     */
    private Collection<TypeImpl> getSubConceptTypes(){
        Collection<TypeImpl> subSet = new HashSet<>();
        getIncomingNeighbours(Schema.EdgeLabel.AKO).forEach(concept -> subSet.add(getMindmapsGraph().getElementFactory().buildSpecificConceptType(concept)));
        return subSet;
    }

    /**
     *
     * @return All the instances of this type.
     */
    @SuppressWarnings("unchecked")
    @Override
    public Collection<V> instances() {
        Set<V> instances = new HashSet<>();

        //noinspection unchecked
        GraphTraversal<Vertex, Vertex> traversal = getMindmapsGraph().getTinkerPopGraph().traversal().V()
                .has(Schema.ConceptProperty.ITEM_IDENTIFIER.name(), getId())
                .union(__.identity(), __.repeat(__.in(Schema.EdgeLabel.AKO.getLabel())).emit()).unfold()
                .in(Schema.EdgeLabel.ISA.getLabel())
                .union(__.identity(), __.repeat(__.in(Schema.EdgeLabel.AKO.getLabel())).emit()).unfold();

        traversal.forEachRemaining(vertex -> {
            ConceptImpl concept = getMindmapsGraph().getElementFactory().buildUnknownConcept(vertex);
            if(!Schema.BaseType.CASTING.name().equals(concept.getBaseType())){
                instances.add((V) concept);
            }
        });

        return instances;
    }

    /**
     *
     * @return returns true if the type is set to be abstract.
     */
    @Override
    public Boolean isAbstract() {
        Object object = getProperty(Schema.ConceptProperty.IS_ABSTRACT);
        return object != null && Boolean.parseBoolean(object.toString());
    }

    /**
     *
     * @return A collection of Rules for which this Type serves as a hypothesis
     */
    @Override
    public Collection<Rule> getRulesOfHypothesis() {
        Set<Rule> rules = new HashSet<>();
        getIncomingNeighbours(Schema.EdgeLabel.HYPOTHESIS).forEach(concept -> rules.add(getMindmapsGraph().getElementFactory().buildRule(concept)));
        return rules;
    }

    /**
     *
     * @return A collection of Rules for which this Type serves as a conclusion
     */
    @Override
    public Collection<Rule> getRulesOfConclusion() {
        Set<Rule> rules = new HashSet<>();
        getIncomingNeighbours(Schema.EdgeLabel.CONCLUSION).forEach(concept -> rules.add(getMindmapsGraph().getElementFactory().buildRule(concept)));
        return rules;
    }

    /**
     *
     * @param type This type's super type
     * @return The Type itself
     */
    public T superType(T type) {
        //Track any existing data if there is some
        Type currentSuperType = superType();
        if(currentSuperType != null){
            currentSuperType.instances().forEach(concept -> {
                if(concept.isInstance()){
                    ((InstanceImpl<?, ?>) concept).castings().forEach(
                            instance -> mindmapsGraph.getConceptLog().putConcept(instance));
                }
            });
        }

        deleteEdges(Direction.OUT, Schema.EdgeLabel.AKO);
        deleteEdges(Direction.OUT, Schema.EdgeLabel.ISA);
        putEdge(getMindmapsGraph().getElementFactory().buildSpecificConceptType(type), Schema.EdgeLabel.AKO);
        type(); //Check if there is a circular ako loop
        return getThis();
    }

    /**
     *
     * @param roleType The Role Type which the instances of this Type are allowed to play.
     * @return The Type itself.
     */
    @Override
    public T playsRole(RoleType roleType) {
        putEdge(getMindmapsGraph().getElementFactory().buildRoleType(roleType), Schema.EdgeLabel.PLAYS_ROLE);
        return getThis();
    }

    /**
     *
     * @param roleType The Role Type which the instances of this Type should no longer be allowed to play.
     * @return The Type itself.
     */
    @Override
    public T deletePlaysRole(RoleType roleType) {
        deleteEdgeTo(Schema.EdgeLabel.PLAYS_ROLE, getMindmapsGraph().getElementFactory().buildRoleType(roleType));

        //Add castings to tracking to make sure they can still be played.
        instances().forEach(concept -> {
            if (concept.isInstance()) {
                ((InstanceImpl<?, ?>) concept).castings().forEach(casting -> mindmapsGraph.getConceptLog().putConcept(casting));
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

    /**
     *
     * @param isAbstract  Specifies if the concept is abstract (true) or not (false).
     *                    If the concept type is abstract it is not allowed to have any instances.
     * @return The Type itself.
     */
    @Override
    public T setAbstract(Boolean isAbstract) {
        setProperty(Schema.ConceptProperty.IS_ABSTRACT, isAbstract);
        if(isAbstract)
            mindmapsGraph.getConceptLog().putConcept(this);
        return getThis();
    }
}
