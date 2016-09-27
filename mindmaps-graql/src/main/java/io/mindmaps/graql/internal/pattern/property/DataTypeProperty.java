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

public class DataTypeProperty extends AbstractNamedProperty {

    private final ResourceType.DataType<?> datatype;

    public DataTypeProperty(ResourceType.DataType<?> datatype) {
        this.datatype = datatype;
    }

    public ResourceType.DataType<?> getDatatype() {
        return datatype;
    }

    @Override
    protected String getName() {
        return "datatype";
    }

    @Override
    protected String getProperty() {
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
}
