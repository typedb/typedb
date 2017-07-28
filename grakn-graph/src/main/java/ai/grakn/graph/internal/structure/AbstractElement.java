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
package ai.grakn.graph.internal.structure;

import ai.grakn.concept.Concept;
import ai.grakn.exception.GraphOperationException;
import ai.grakn.exception.PropertyNotUniqueException;
import ai.grakn.graph.internal.AbstractGraknGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Function;

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
 * @param <E> The type of the element. Either {@link VertexElement} of {@link EdgeElement}
 * @param <P> Enum indicating the allowed properties on each type. Either {@link ai.grakn.util.Schema.VertexProperty} or
 *           {@link ai.grakn.util.Schema.EdgeProperty}
 *
 * @author fppt
 *
 */
public abstract class AbstractElement<E extends Element, P extends Enum> {
    private final String prefix;
    private final E element;
    private final AbstractGraknGraph graknGraph;

    AbstractElement(AbstractGraknGraph graknGraph, E element, String prefix){
        this.graknGraph = graknGraph;
        this.element = element;
        this.prefix = prefix;
    }

    public E element(){
        return element;
    }

    public ElementId id(){
        return ElementId.of(prefix + element().id());
    }

    /**
     * Deletes the element from the graph
     */
    public void delete(){
        element().remove();
    }

    /**
     *
     * @param key The key of the property to mutate
     * @param value The value to commit into the property
     */
    public void property(P key, Object value){
        if(value == null) {
            element().property(key.name()).remove();
        } else {
            Property<Object> foundProperty = element().property(key.name());
            if(!foundProperty.isPresent() || !foundProperty.value().equals(value)){
                element().property(key.name(), value);
            }
        }
    }

    /**
     *
     * @param key The key of the non-unique property to retrieve
     * @return The value stored in the property
     */
    @Nullable
    public <X> X property(P key){
        Property<X> property = element().property(key.name());
        if(property != null && property.isPresent()) {
            return property.value();
        }
        return null;
    }
    public Boolean propertyBoolean(P key){
        Boolean value = property(key);
        if(value == null) return false;
        return value;
    }

    /**
     *
     * @return The grakn graph this concept is bound to.
     */
    public AbstractGraknGraph<?> graph() {
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
     * Sets the value of a property with the added restriction that no other vertex can have that property.
     *
     * @param key The key of the unique property to mutate
     * @param value The new value of the unique property
     */
    public void propertyUnique(P key, String value){
        if(!graph().isBatchGraph()) {
            GraphTraversal<Vertex, Vertex> traversal = graph().getTinkerTraversal().V().has(key.name(), value);
            if(traversal.hasNext()) throw PropertyNotUniqueException.cannotChangeProperty(element(), traversal.next(), key, value);
        }

        property(key, value);
    }

    /**
     * Sets a property which cannot be mutated
     *
     * @param property The key of the immutable property to mutate
     * @param newValue The new value to put on the property (if the property is not set)
     * @param foundValue The current value of the property
     * @param converter Helper method to ensure data is persisted in the correct format
     */
    public <X> void propertyImmutable(P property, X newValue, @Nullable X foundValue, Function<X, Object> converter){
        Objects.requireNonNull(property);

        if(foundValue != null){
            if(!foundValue.equals(newValue)){
                throw GraphOperationException.immutableProperty(foundValue, newValue, property);
            }
        } else {
            property(property, converter.apply(newValue));
        }
    }

    public <X> void propertyImmutable(P property, X newValue, X foundValue){
        propertyImmutable(property, newValue, foundValue, Function.identity());
    }

    /**
     *
     * @return the label of the element in the graph.
     */
    public String label(){
        return element().label();
    }
}
