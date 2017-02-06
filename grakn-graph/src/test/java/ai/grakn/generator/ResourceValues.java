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
 *
 */

package ai.grakn.generator;

import ai.grakn.concept.ResourceType;

/**
 * Generator that generates random valid resource values.
 */
public class ResourceValues extends AbstractGenerator<Object> {

    private ResourceType.DataType<?> dataType = null;

    public ResourceValues() {
        super(Object.class);
    }

    @Override
    public Object generate() {
        String type;
        if (dataType == null) {
            type = random.choose(ResourceType.DataType.SUPPORTED_TYPES.keySet());
        } else {
            type = dataType.getName();
        }

        switch (type) {
            case "java.lang.String":
                return gen(String.class);
            case "java.lang.Boolean":
                return gen(Boolean.class);
            case "java.lang.Integer":
            case "java.lang.Long":
                return gen(Long.class);
            case "java.lang.Double":
                return gen(Double.class);
            default:
                throw new RuntimeException("unreachable: " + type);
        }
    }

    ResourceValues dataType(ResourceType.DataType<?> dataType) {
        this.dataType = dataType;
        return this;
    }

}
