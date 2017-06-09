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
 * <p>
 *     Represent an Edge in a Grakn Graph
 * </p>
 *
 * <p>
 *    Wraps a tinkerpop {@link Edge} constraining it to the Grakn Object Model.
 * </p>
 *
 * @author fppt
 */
class EdgeElement extends AbstractElement<Edge> {

    EdgeElement(AbstractGraknGraph graknGraph, Edge e){
        super(graknGraph, e);
    }

    /**
     * Deletes the edge between two concepts and adds both those concepts for re-validation in case something goes wrong
     */
    public void delete(){
        element().remove();
    }

    @Override
    public int hashCode() {
        return element().hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        EdgeElement edge = (EdgeElement) object;

        return element().id().equals(edge.id());
    }

    /**
     *
     * @return The source of the edge.
     */
    public <X extends Concept> X getSource(){
        return graph().getElementFactory().buildConcept(element().outVertex());
    }

    /**
     *
     * @return The target of the edge
     */
    public <X extends Concept> X getTarget(){
        return graph().getElementFactory().buildConcept(element().inVertex());
    }

    /**
     *
     * @return The type of the edge
     */
    public Schema.EdgeLabel getLabel() {
        return Schema.EdgeLabel.getEdgeLabel(element().label());
    }

    /**
     *
     * @param key The property to retrieve
     * @return The value of the property
     */
    <X> X getProperty(Schema.EdgeProperty key){
        return getProperty(key.name());
    }
    Boolean getPropertyBoolean(Schema.EdgeProperty key){
        return getPropertyBoolean(key.name());
    }

    /**
     *
     * @param type The property to retrieve
     * @param value The value of the property
     */
    void setProperty(Schema.EdgeProperty type, Object value){
        setProperty(type.name(), value);
    }

}
