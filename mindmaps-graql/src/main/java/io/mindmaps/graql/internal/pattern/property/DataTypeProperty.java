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

package io.mindmaps.graql.internal.pattern.property;

import io.mindmaps.concept.ResourceType;
import io.mindmaps.graql.admin.UniqueVarProperty;
import io.mindmaps.graql.internal.gremlin.FragmentPriority;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static io.mindmaps.util.Schema.ConceptProperty.DATA_TYPE;

public class DataTypeProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty, SingleTraversalProperty {

    private final ResourceType.DataType<?> datatype;

    public DataTypeProperty(ResourceType.DataType<?> datatype) {
        this.datatype = datatype;
    }

    public ResourceType.DataType<?> getDatatype() {
        return datatype;
    }

    @Override
    public String getName() {
        return "datatype";
    }

    @Override
    public String getProperty() {
        if (datatype == ResourceType.DataType.BOOLEAN) {
            return "boolean";
        } else if (datatype == ResourceType.DataType.DOUBLE) {
            return "double";
        } else if (datatype == ResourceType.DataType.LONG) {
            return "long";
        } else if (datatype == ResourceType.DataType.STRING) {
            return "string";
        } else {
            throw new RuntimeException("Unknown data type: " + datatype.getName());
        }
    }

    @Override
    public GraphTraversal<Vertex, Vertex> applyTraversal(GraphTraversal<Vertex, Vertex> traversal) {
        return traversal.has(DATA_TYPE.name(), P.eq(datatype.getName()));
    }

    @Override
    public FragmentPriority getPriority() {
        return FragmentPriority.VALUE_NONSPECIFIC;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        DataTypeProperty that = (DataTypeProperty) o;

        return datatype.equals(that.datatype);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + datatype.hashCode();
        return result;
    }
}
