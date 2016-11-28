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

import ai.grakn.concept.Concept;
import ai.grakn.util.Schema;
import org.apache.tinkerpop.gremlin.structure.Edge;

/**
 * Wraps a tinkerpop edge constraining it to our object concept
 */
class EdgeImpl {
    private Edge edge;
    private final AbstractGraknGraph graknGraph;

    EdgeImpl(org.apache.tinkerpop.gremlin.structure.Edge e, AbstractGraknGraph graknGraph){
        edge = e;
        this.graknGraph = graknGraph;
    }

    /**
     * Deletes the edge between two concepts and adds both those concepts for re-validation in case something goes wrong
     */
    public void delete(){
        graknGraph.getConceptLog().putConcept(getTarget());
        graknGraph.getConceptLog().putConcept(getSource());

        edge.remove();
        edge = null;
    }

    @Override
    public int hashCode() {
        return edge.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof EdgeImpl && ((EdgeImpl) object).edgeEquals(edge);
    }

    /**
     *
     * @return The source of the edge.
     */
    public <X extends Concept> X getSource(){
        return graknGraph.getElementFactory().buildUnknownConcept(edge.outVertex());
    }

    /**
     *
     * @return The target of the edge
     */
    public <X extends Concept> X getTarget(){
        return graknGraph.getElementFactory().buildUnknownConcept(edge.inVertex());
    }

    /**
     *
     * @return The type of the edge
     */
    public Schema.EdgeLabel getType() {
        return Schema.EdgeLabel.getEdgeLabel(edge.label());
    }

    /**
     *
     * @param type The property to retrieve
     * @return The value of the property
     */
    <X> X getProperty(Schema.EdgeProperty type){
        org.apache.tinkerpop.gremlin.structure.Property<X> property = edge.property(type.name());
        if(property != null && property.isPresent())
            return property.value();
        else
            return null;
    }

    Boolean getPropertyBoolean(Schema.EdgeProperty key){
        Boolean value = getProperty(key);
        if(value == null)
            return false;
        return value;
    }

    /**
     *
     * @param type The property to retrieve
     * @param value The value of the property
     */
    void setProperty(Schema.EdgeProperty type, Object value){
        edge.property(type.name(), value);
    }

    private boolean edgeEquals(org.apache.tinkerpop.gremlin.structure.Edge toEdge) {
        return edge.equals(toEdge);
    }
}
