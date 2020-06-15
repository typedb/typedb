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

import grakn.core.core.Schema;
import grakn.core.kb.concept.structure.EdgeElement;
import grakn.core.kb.concept.structure.GraknElementException;
import grakn.core.kb.concept.structure.VertexElement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Function;

/**
 * Represent an Edge in a TransactionOLTP
 * Wraps a tinkerpop Edge constraining it to the Grakn Object Model.
 */
public class EdgeElementImpl extends AbstractElementImpl<Edge> implements EdgeElement {

    public EdgeElementImpl(ElementFactory elementFactory, Edge e) {
        super(elementFactory, e);
    }

    /**
     * Deletes the edge between two concepts and adds both those concepts for re-validation in case something goes wrong
     */
    public void delete() {
        element().remove();
    }

    /**
     * @param key The key of the non-unique property to retrieve
     * @return The value stored in the property
     */
    @Override
    @Nullable
    public <X> X property(Schema.EdgeProperty key) {
        Property<X> property = element().property(key.name());
        if (property != null && property.isPresent()) {
            return property.value();
        }
        return null;
    }

    @Override
    public Boolean propertyBoolean(Schema.EdgeProperty key) {
        Boolean value = property(key);
        if (value == null) return false;
        return value;
    }

    /**
     * @param key   The key of the property to mutate
     * @param value The value to commit into the property
     */
    @Override
    public void property(Schema.EdgeProperty key, Object value) {
        element().property(key.name()).remove();
        if (value != null) {
            element().property(key.name(), value);
        }
    }

    @Override
    public int hashCode() {
        return element().hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;

        grakn.core.concept.structure.EdgeElementImpl edge = (EdgeElementImpl) object;

        return element().id().equals(edge.id());
    }

    public VertexElement source() {
        return elementFactory.buildVertexElement(element().outVertex());
    }

    public VertexElement target() {
        return elementFactory.buildVertexElement(element().inVertex());
    }
}
