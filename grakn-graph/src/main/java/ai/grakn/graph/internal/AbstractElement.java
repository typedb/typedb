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
import ai.grakn.exception.GraphOperationException;
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

    E getElement(){
        return element;
    }

    ElementId getElementId(){
        return ElementId.of(getElement().id());
    }

    /**
     *
     * @param key The key of the property to mutate
     * @param value The value to commit into the property
     */
     void setProperty(String key, Object value){
        if(value == null) {
            getElement().property(key).remove();
        } else {
            Property<Object> foundProperty = getElement().property(key);
            if(!foundProperty.isPresent() || !foundProperty.value().equals(value)){
                getElement().property(key, value);
            }
        }
    }

    /**
     *
     * @param key The key of the non-unique property to retrieve
     * @return The value stored in the property
     */
    public <X> X getProperty(String key){
        Property<X> property = getElement().property(key);
        if(property != null && property.isPresent()) {
            return property.value();
        }
        return null;
    }
    Boolean getPropertyBoolean(String key){
        Boolean value = getProperty(key);
        if(value == null) return false;
        return value;
    }

    /**
     *
     * @return The grakn graph this concept is bound to.
     */
    protected AbstractGraknGraph<?> getGraknGraph() {
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
        return this == object || object instanceof AbstractElement && ((AbstractElement) object).getElementId().equals(getElementId());
    }

    /**
     * Helper method to cast a concept to it's correct type
     * @param type The type to cast to
     * @param <E> The type of the interface we are casting to.
     * @return The concept itself casted to the defined interface
     * @throws GraphOperationException when casting an element incorrectly
     */
    private <E extends AbstractElement> E castConcept(Class<E> type){
        try {
            return type.cast(this);
        } catch(ClassCastException e){
            throw GraphOperationException.invalidCasting(this, type);
        }
    }

    /**
     *
     * @return the label of the element in the graph.
     */
    String label(){
        return getElement().label();
    }

    /**
     *
     * @return A roleplayer if the element is a roleplayer
     */
    public Casting asCasting(){
        return castConcept(Casting.class);
    }

    /**
     *
     * @return true if the element is a Casting
     */
    public boolean isCasting(){
        return this instanceof Casting;
    }
}
