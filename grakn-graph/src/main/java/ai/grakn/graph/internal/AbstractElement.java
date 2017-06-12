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
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import static org.apache.tinkerpop.gremlin.structure.T.id;

/**
 * <p>
 *     Graph AbstractElement
 * </p>
 *
 * <p>
 *     Base class used to represent a construct in the graph. This includes exposed constructs such as {@link Concept}
 *     and hidden constructs such as {@link EdgeElement} and {@link Casting}
 * </p>
 *
 * @author fppt
 *
 */
abstract class AbstractElement<E extends Element> {
    private final E element;
    private final AbstractGraknGraph graknGraph;

    AbstractElement(AbstractGraknGraph graknGraph, E element){
        this.graknGraph = graknGraph;
        this.element = element;
    }

    E element(){
        return element;
    }

    ElementId id(){
        return ElementId.of(element().id());
    }

    /**
     * Deletes the element from the graph
     */
    void delete(){
        element().remove();
    }

    /**
     *
     * @param key The key of the property to mutate
     * @param value The value to commit into the property
     */
     void property(String key, Object value){
        if(value == null) {
            element().property(key).remove();
        } else {
            Property<Object> foundProperty = element().property(key);
            if(!foundProperty.isPresent() || !foundProperty.value().equals(value)){
                element().property(key, value);
            }
        }
    }

    /**
     *
     * @param key The key of the non-unique property to retrieve
     * @return The value stored in the property
     */
    public <X> X property(String key){
        Property<X> property = element().property(key);
        if(property != null && property.isPresent()) {
            return property.value();
        }
        return null;
    }
    Boolean propertyBoolean(String key){
        Boolean value = property(key);
        if(value == null) return false;
        return value;
    }

    /**
     *
     * @return The grakn graph this concept is bound to.
     */
    protected AbstractGraknGraph<?> graph() {
        return graknGraph;
    }

    /**
     *
     * @return The hash code of the underlying vertex
     */
    public int hashCode() {
        return id.hashCode(); //Note: This means that concepts across different transactions will be equivalent.
    }

    /**
     *
     * @return true if the elements equal each other
     */
    @Override
    public boolean equals(Object object) {
        //Compare Concept
        //based on id because vertex comparisons are equivalent
        return this == object || object instanceof AbstractElement && ((AbstractElement) object).id().equals(id());
    }

    /**
     *
     * @return the label of the element in the graph.
     */
    String label(){
        return element().label();
    }
}
