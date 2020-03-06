/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */
package grakn.core.concept.structure;

import grakn.core.kb.concept.structure.AbstractElement;
import org.apache.tinkerpop.gremlin.structure.Element;

import static org.apache.tinkerpop.gremlin.structure.T.id;

/**
 * TransactionOLTP AbstractElement
 * Base class used to represent a construct in the graph. This includes exposed constructs such as Concept
 * and hidden constructs such as EdgeElement and Casting
 *
 * @param <E> The type of the element. Either VertexElement of EdgeElement
 */
public abstract class AbstractElementImpl<E extends Element> implements AbstractElement<E> {
    private E element;
    final ElementFactory elementFactory;

    AbstractElementImpl(ElementFactory elementFactory, E element) {
        this.elementFactory = elementFactory;
        this.element = element;
    }

    @Override
    public E element() {
        return element;
    }

    @Override
    public Object id() {
        return element().id();
    }

    /**
     * Deletes the element from the graph
     */
    @Override
    public void delete() {
        /*
        Here we force a re-fetch from janus right before deleting an element
        This is because the element contained within this object may have been evicted from the Janus transaction cache -
        doing the re-read here brings a new copy back in before deleting in, which then pins it to the cache for
        the remainder of the transaction
         */

        // TODO check this is fixed for sure
//        if (this instanceof VertexElement) {
//            element = (E)tx.getTinkerTraversal().V(id()).next();
//        } else {
//            element = (E)tx.getTinkerTraversal().E(id()).next();
//        }
        element().remove();
    }


    /**
     * @return The hash code of the underlying vertex
     */
    @Override
    public int hashCode() {
        return id.hashCode(); //Note: This means that concepts across different transactions will be equivalent.
    }

    /**
     * @return true if the elements equal each other
     */
    @Override
    public boolean equals(Object object) {
        //Compare Concept
        //based on id because vertex comparisons are equivalent
        return this == object || object instanceof AbstractElementImpl && ((AbstractElementImpl) object).id().equals(id());
    }

    /**
     * @return the label of the element in the graph.
     */
    @Override
    public String label() {
        return element().label();
    }

    @Override
    public final boolean isDeleted() {
        return !ElementUtils.isValidElement(element());
    }


}
