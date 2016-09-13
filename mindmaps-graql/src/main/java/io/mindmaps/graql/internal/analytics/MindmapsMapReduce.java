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

package io.mindmaps.graql.internal.analytics;

import io.mindmaps.util.ErrorMessage;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * A map reduce task specific to Mindmaps with common method implementations.
 */
public abstract class MindmapsMapReduce<T> extends CommonOLAP implements MapReduce<Serializable, T, Serializable, T, Map<Serializable, T>>  {
    static final String MAP_REDUCE_MEMORY_KEY = "MindmapsMapReduce.memoryKey";

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);

        // store class name for reflection on spark executor
        configuration.setProperty(MAP_REDUCE, this.getClass().getName());
    }

    @Override
    public String getMemoryKey() {
        return MAP_REDUCE_MEMORY_KEY;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    @Override
    public MapReduce<Serializable, T, Serializable, T, Map<Serializable, T>> clone() {
        try {
            final MindmapsMapReduce clone = (MindmapsMapReduce) super.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(ErrorMessage.CLONE_FAILED.getMessage(this.getClass().toString(),e.getMessage()),e);
        }
    }
}
