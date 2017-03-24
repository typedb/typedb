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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

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
        String className;
        if (dataType == null) {
            className = random.choose(ResourceType.DataType.SUPPORTED_TYPES.keySet());
        } else {
            className = dataType.getName();
        }

        Class<?> clazz;

        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unrecognised class " + className);
        }

        if(clazz.equals(LocalDateTime.class)){
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(random.nextLong()), ZoneId.systemDefault());
        }

        return gen(clazz);
    }

    ResourceValues dataType(ResourceType.DataType<?> dataType) {
        this.dataType = dataType;
        return this;
    }

}
