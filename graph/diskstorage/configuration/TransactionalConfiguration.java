/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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
 */

package grakn.core.graph.diskstorage.configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import grakn.core.graph.diskstorage.configuration.ConcurrentWriteConfiguration;
import grakn.core.graph.diskstorage.configuration.WriteConfiguration;
import grakn.core.graph.graphdb.database.idhandling.VariableLong;
import grakn.core.graph.graphdb.database.serialize.DataOutput;

import java.util.HashMap;
import java.util.Map;


public class TransactionalConfiguration implements WriteConfiguration {

    private final WriteConfiguration config;

    private final Map<String, Object> readValues;
    private final Map<String, Object> writtenValues;

    public TransactionalConfiguration(WriteConfiguration config) {
        Preconditions.checkNotNull(config);
        this.config = config;
        this.readValues = new HashMap<>();
        this.writtenValues = new HashMap<>();
    }

    @Override
    public <O> void set(String key, O value) {
        writtenValues.put(key, value);
    }

    @Override
    public void remove(String key) {
        writtenValues.put(key, null);
    }

    @Override
    public WriteConfiguration copy() {
        return config.copy();
    }

    @Override
    public <O> O get(String key, Class<O> datatype) {
        Object value = writtenValues.get(key);
        if (value != null) return (O) value;
        value = readValues.get(key);
        if (value != null) return (O) value;
        value = config.get(key, datatype);
        readValues.put(key, value);
        return (O) value;
    }

    @Override
    public Iterable<String> getKeys(String prefix) {
        return Iterables.concat(
                Iterables.filter(writtenValues.keySet(), s -> s.startsWith(prefix)),
                Iterables.filter(config.getKeys(prefix), s -> !writtenValues.containsKey(s)));
    }

    public void commit() {
        for (Map.Entry<String, Object> entry : writtenValues.entrySet()) {
            if (config instanceof ConcurrentWriteConfiguration && readValues.containsKey(entry.getKey())) {
                ((ConcurrentWriteConfiguration) config).set(entry.getKey(), entry.getValue(), readValues.get(entry.getKey()));
            } else {
                config.set(entry.getKey(), entry.getValue());
            }
        }
        rollback();
    }

    public void rollback() {
        writtenValues.clear();
        readValues.clear();
    }

    public boolean hasMutations() {
        return !writtenValues.isEmpty();
    }

    public void logMutations(DataOutput out) {
        Preconditions.checkArgument(hasMutations());
        VariableLong.writePositive(out, writtenValues.size());
        for (Map.Entry<String, Object> entry : writtenValues.entrySet()) {
            out.writeObjectNotNull(entry.getKey());
            out.writeClassAndObject(entry.getValue());
        }
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }
}
