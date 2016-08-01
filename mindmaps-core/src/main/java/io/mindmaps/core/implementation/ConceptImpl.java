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

import io.mindmaps.core.exceptions.*;
import io.mindmaps.core.model.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.HashSet;
import java.util.Set;

/**
 * A concept node which wraps around the vertex. This gives grater control over accessible properties.
 */
abstract class ConceptImpl<T extends Concept, V extends Type, D> implements Concept {
    @SuppressWarnings("unchecked")
    T getThis(){
        return (T) this;
    }

    final MindmapsTransactionImpl mindmapsTransaction;
    private Vertex vertex;

    ConceptImpl(Vertex v, MindmapsTransactionImpl mindmapsTransaction){
        this.vertex = v;
        this.mindmapsTransaction = mindmapsTransaction;
        mindmapsTransaction.getTransaction().putConcept(this);
    }

    //Root Set and Get
    private T setProperty(String key, Object value){
        if(value == null)
            vertex.property(key).remove();
        else
            vertex.property(key, value);
        return getThis();
    }
    private Object getProperty(String key){
        VertexProperty property = vertex.property(key);
        if(property != null && property.isPresent())
            return property.value();
        else
            return null;
    }

    @Override
    public void delete() throws ConceptException {
        ConceptImpl properType = getMindmapsTransaction().getElementFactory().buildUnknownConcept(this);
        properType.innerDelete(); //This will execute the proper deletion method.
    }
    void innerDelete(){
        deleteNode();
    }

    @Override
    public T setId(String id) {
        if(DataType.ConceptMeta.isMetaId(id)){
            throw new ConceptException(ErrorMessage.ID_RESERVED.getMessage(id));
        }

        return setUniqueProperty(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER, id);
    }

    @Override
    public T setSubject(String subject) {
        return setUniqueProperty(DataType.ConceptPropertyUnique.SUBJECT_IDENTIFIER, subject);
    }

    T setUniqueProperty(DataType.ConceptPropertyUnique key, String id){
        if(mindmapsTransaction.isBatchLoadingEnabled() || updateAllowed(key, id))
            return setProperty(key, id);
        else
            throw new ConceptIdNotUniqueException(this, key, id);
    }

    private boolean updateAllowed(DataType.ConceptPropertyUnique key, String value) {
        ConceptImpl fetchedConcept = mindmapsTransaction.getConcept(key, value);
        return fetchedConcept == null || this.equals(fetchedConcept);
    }

    void deleteNode(){
        // tracking
        vertex.edges(Direction.BOTH).
                forEachRemaining(
                        e -> {
                            mindmapsTransaction.getTransaction().putConcept(getMindmapsTransaction().getElementFactory().buildUnknownConcept(e.inVertex()));
                            mindmapsTransaction.getTransaction().putConcept(getMindmapsTransaction().getElementFactory().buildUnknownConcept(e.outVertex()));}
                );
        mindmapsTransaction.getTransaction().removeConcept(this);
        // delete node
        vertex.remove();
        vertex = null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V type() {
        HashSet<Concept> visitedConcepts = new HashSet<>();
        ConceptImpl currentConcept = this;
        visitedConcepts.add(currentConcept);
        Type type = null;
        boolean notFound = true;

        while(notFound){
            ConceptImpl concept = currentConcept.getParentIsa();
            if(concept != null){
                //Checks the following case c1 -ako-> c2 -ako-> c3 -isa-> c1 is invalid
                if(visitedConcepts.contains(concept) && !concept.equals(currentConcept)){
                    throw new ConceptException(ErrorMessage.LOOP_DETECTED.getMessage(toString(), DataType.EdgeLabel.AKO.getLabel() + " " + DataType.EdgeLabel.ISA.getLabel()));
                }
                notFound = false;
                type = getMindmapsTransaction().getElementFactory().buildSpecificConceptType(concept);
            } else {
                currentConcept = currentConcept.getParentAko();
                if(visitedConcepts.contains(currentConcept)){
                    throw new ConceptException(ErrorMessage.LOOP_DETECTED.getMessage(toString(), DataType.EdgeLabel.AKO.getLabel() + " " + DataType.EdgeLabel.ISA.getLabel()));
                }
                visitedConcepts.add(currentConcept);

            }
        }

        return (V) type;
    }

    private <E> E castConcept(Class<E> type){
        try {
            return type.cast(this);
        } catch(ClassCastException e){
            throw new InvalidConceptTypeException(this, type);
        }
    }

    @Override
    public Type asType() {
        return castConcept(Type.class);
    }

    @Override
    public Instance asInstance() {
        return castConcept(Instance.class);
    }

    @Override
    public EntityType asEntityType() {
        return castConcept(EntityType.class);
    }

    @Override
    public RoleType asRoleType() {
        return castConcept(RoleType.class);
    }

    @Override
    public RelationType asRelationType() {
        return castConcept(RelationType.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <D> ResourceType<D> asResourceType() {
        return castConcept(ResourceType.class);
    }

    @Override
    public RuleType asRuleType() {
        return castConcept(RuleType.class);
    }

    @Override
    public Entity asEntity() {
        return castConcept(Entity.class);
    }

    @Override
    public Relation asRelation() {
        return castConcept(Relation.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <D> Resource<D> asResource() {
        return castConcept(Resource.class);
    }

    @Override
    public Rule asRule() {
        return castConcept(Rule.class);
    }

    public CastingImpl asCasting(){
        return (CastingImpl) this;
    }

    @Override
    public boolean isType() {
        return this instanceof Type;
    }

    @Override
    public boolean isInstance() {
        return this instanceof Instance;
    }

    @Override
    public boolean isEntityType() {
        return this instanceof EntityType;
    }

    @Override
    public boolean isRoleType() {
        return this instanceof RoleType;
    }

    @Override
    public boolean isRelationType() {
        return this instanceof RelationType;
    }

    @Override
    public boolean isResourceType() {
        return this instanceof ResourceType;
    }

    @Override
    public boolean isRuleType() {
        return this instanceof RuleType;
    }

    @Override
    public boolean isEntity() {
        return this instanceof Entity;
    }

    @Override
    public boolean isRelation() {
        return this instanceof Relation;
    }

    @Override
    public boolean isResource() {
        return this instanceof Resource;
    }

    @Override
    public boolean isRule() {
        return this instanceof Rule;
    }

    public boolean isCasting(){
        return this instanceof CastingImpl;
    }

    public T type(Type type) {
        deleteEdges(Direction.OUT, DataType.EdgeLabel.ISA);
        putEdge(getMindmapsTransaction().getElementFactory().buildSpecificConceptType(type), DataType.EdgeLabel.ISA);
        setType(String.valueOf(type.getId()));

        //Put any castings back into tracking to make sure the type is still valid
        getIncomingNeighbours(DataType.EdgeLabel.ROLE_PLAYER).forEach(casting -> {
            mindmapsTransaction.getTransaction().putConcept(casting);
        });

        return getThis();
    }


    public Set<Type> getConceptTypeHierarchy() {
        HashSet<Type> types = new HashSet<>();
        Concept currentConcept = this;
        boolean hasMoreParents = true;
        while(hasMoreParents){
            Type type = currentConcept.type();
            if(type == null || types.contains(type)){
                hasMoreParents = false;
            } else {
                types.add(type);
                currentConcept = type;
            }
        }
        return types;
    }

    public TypeImpl getParentIsa(){
        Concept isaParent = getOutgoingNeighbour(DataType.EdgeLabel.ISA);
        if(isaParent != null){
            return getMindmapsTransaction().getElementFactory().buildSpecificConceptType(isaParent);
        } else {
            return null;
        }
    }

    public TypeImpl getParentAko(){
        Concept akoParent = getOutgoingNeighbour(DataType.EdgeLabel.AKO);
        if(akoParent != null){
            return getMindmapsTransaction().getElementFactory().buildSpecificConceptType(akoParent);
        } else {
            return null;
        }
    }

    protected Concept getOutgoingNeighbour(DataType.EdgeLabel edgeLabel){
        Set<ConceptImpl> concepts = getOutgoingNeighbours(edgeLabel);
        if(concepts.size() == 1){
            return concepts.iterator().next();
        } else if(concepts.isEmpty()){
            return null;
        } else {
            throw new MoreThanOneEdgeException(this, edgeLabel);
        }
    }

    protected Set<ConceptImpl> getOutgoingNeighbours(DataType.EdgeLabel edgeType){
        Set<ConceptImpl> outgoingNeighbours = new HashSet<>();

        getEdgesOfType(Direction.OUT, edgeType).forEach(edge -> {
            outgoingNeighbours.add(edge.getToConcept());
        });
        return outgoingNeighbours;
    }

    Concept getIncomingNeighbour(DataType.EdgeLabel edgeLabel){
        Set<ConceptImpl> concepts = getIncomingNeighbours(edgeLabel);
        if(concepts.size() == 1){
            return concepts.iterator().next();
        } else if(concepts.isEmpty()){
            return null;
        } else {
            throw new MoreThanOneEdgeException(this, edgeLabel);
        }
    }
    protected Set<ConceptImpl> getIncomingNeighbours(DataType.EdgeLabel edgeType){
        Set<ConceptImpl> incomingNeighbours = new HashSet<>();
        getEdgesOfType(Direction.IN, edgeType).forEach(edge -> {
            incomingNeighbours.add(edge.getFromConcept());
        });
        return incomingNeighbours;
    }

    //Root Set and Get
    public T setProperty(DataType.ConceptPropertyUnique key, Object value) {
        return setProperty(key.name(), value);
    }
    T setProperty(DataType.ConceptProperty key, Object value){
        return setProperty(key.name(), value);
    }

    public String getProperty(DataType.ConceptPropertyUnique key){
        Object property = getProperty(key.name());
        if(property == null)
            return null;
        else
            return property.toString();
    }
    public Object getProperty(DataType.ConceptProperty key){
        return getProperty(key.name());
    }
    Vertex getVertex() {
        return vertex;
    }

    //------------ Setters ------------
    public Concept setType(String type){
        return setProperty(DataType.ConceptProperty.TYPE, type);
    }


    public T setValue(D value) {
        return setProperty(DataType.ConceptProperty.VALUE_STRING, value);
    }

    //------------ Getters ------------
    public long getBaseIdentifier() {
        return (long) vertex.id();
    }
    public String getBaseType(){
        return vertex.label();
    }
    @Override
    public String getId(){
        return getProperty(DataType.ConceptPropertyUnique.ITEM_IDENTIFIER);
    }
    @Override
    public String getSubject() {
        return getProperty(DataType.ConceptPropertyUnique.SUBJECT_IDENTIFIER);
    }
    public String getType(){
        return String.valueOf(getProperty(DataType.ConceptProperty.TYPE));
    }

    @SuppressWarnings("unchecked")
    public D getValue() {
        return (D) getProperty(DataType.ConceptProperty.VALUE_STRING);
    }

    protected Set<EdgeImpl> getEdgesOfType(Direction direction, DataType.EdgeLabel type){
        Set<EdgeImpl> edges = new HashSet<>();
        vertex.edges(direction, type.getLabel()).
                forEachRemaining(e -> edges.add(new EdgeImpl(e, getMindmapsTransaction())));
        return edges;
    }

    public EdgeImpl getEdgeOutgoingOfType(DataType.EdgeLabel type) {
        Set<EdgeImpl> edges = getEdgesOfType(Direction.OUT, type);
        if(edges.size() == 1)
            return edges.iterator().next();
        else if(edges.size() > 1)
            throw new MoreThanOneEdgeException(this, type);
        else
            return null;
    }

    MindmapsTransactionImpl getMindmapsTransaction() {return mindmapsTransaction;}

    //--------- Create Links -------//
    void putEdge(ConceptImpl toConcept, DataType.EdgeLabel type){
        GraphTraversal<Vertex, Edge> traversal = mindmapsTransaction.getTinkerPopGraph().traversal().V(getBaseIdentifier()).outE(type.getLabel()).as("edge").otherV().hasId(toConcept.getBaseIdentifier()).select("edge");
        if(!traversal.hasNext())
            addEdge(toConcept, type);
    }

    public EdgeImpl addEdge(ConceptImpl toConcept, DataType.EdgeLabel type) {
        mindmapsTransaction.getTransaction().putConcept(this);
        mindmapsTransaction.getTransaction().putConcept(toConcept);

        return getMindmapsTransaction().getElementFactory().buildEdge(toConcept.addEdgeFrom(this.vertex, type.getLabel()), mindmapsTransaction);
    }

    void deleteEdges(Direction direction, DataType.EdgeLabel type){
        // track changes
        vertex.edges(direction, type.getLabel()).
                forEachRemaining(
                        e -> {
                            mindmapsTransaction.getTransaction().putConcept(
                                    getMindmapsTransaction().getElementFactory().buildUnknownConcept(e.inVertex()));
                            mindmapsTransaction.getTransaction().putConcept(
                                    getMindmapsTransaction().getElementFactory().buildUnknownConcept(e.outVertex()));
                        }
                );

        // deletion
        vertex.edges(direction, type.getLabel()).forEachRemaining(Element::remove);
    }

    void deleteEdgeTo(DataType.EdgeLabel type, ConceptImpl toConcept){
        GraphTraversal<Vertex, Edge> traversal = mindmapsTransaction.getTinkerPopGraph().traversal().V(getBaseIdentifier()).
                outE(type.getLabel()).as("edge").otherV().hasId(toConcept.getBaseIdentifier()).select("edge");
        if(traversal.hasNext())
            traversal.next().remove();
    }

    //====================================================================================
    // By not defining 'public' nor 'private', these functions are only accessible from
    // classes in the same package directory. They are also not exposed in the public
    // interface. Therefore they are not accessible from the rest of the system components.
    private org.apache.tinkerpop.gremlin.structure.Edge addEdgeFrom(Vertex fromVertex, String type) {
        return fromVertex.addEdge(type, vertex);
    }


    //------------ Base Equality ------------
    public int hashCode() {
        return vertex.hashCode();
    }

    private boolean vertexEquals(Vertex toVertex) {
        return vertex.equals(toVertex);
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof ConceptImpl && ((ConceptImpl) object).vertexEquals(vertex);
    }

    @Override
    public String toString(){
        String message = "[" +  this.hashCode() + "] "+
                "- Base Type [" + getBaseType() + "] ";
        if(getSubject() != null)
            message = message +  "- Subject Identifier [" + getSubject() + "] ";
        if(getId() != null)
            message = message + "- Item Identifier [" + getId() + "] ";
        if(getValue() != null)
            message = message + "- Value [" + getValue() + "] ";

        return message;
    }

    //---------- Null Vertex Handler ---------
    public boolean isAlive () {
        return vertex != null;
    }

    @Override
    public int compareTo(Concept o) {
        return this.getId().compareTo(o.getId());
    }
}
