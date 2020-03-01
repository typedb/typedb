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
 */

package grakn.core.graph.diskstorage.configuration.backend;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.configuration.ReadConfiguration;
import grakn.core.graph.diskstorage.configuration.WriteConfiguration;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeySliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.util.BackendOperation;
import grakn.core.graph.diskstorage.util.BufferUtil;
import grakn.core.graph.diskstorage.util.StaticArrayBuffer;
import grakn.core.graph.diskstorage.util.StaticArrayEntry;
import grakn.core.graph.diskstorage.util.time.TimestampProvider;
import grakn.core.graph.graphdb.database.serialize.DataOutput;
import grakn.core.graph.graphdb.database.serialize.StandardSerializer;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class provides READ and WRITE access to Configuration persisted into a KCVStore,
 */

public class KCVSConfiguration implements WriteConfiguration {

    private final BackendOperation.TransactionalProvider txProvider;
    private final TimestampProvider times;
    private final KeyColumnValueStore store;
    private final StaticBuffer rowKey;
    private final StandardSerializer serializer;

    private final Duration maxOperationWaitTime;

    public KCVSConfiguration(BackendOperation.TransactionalProvider txProvider, TimestampProvider timestampProvider, Duration maxOperationWaitTime, KeyColumnValueStore store, String identifier) {
        this.txProvider = txProvider;
        this.times = timestampProvider;
        this.maxOperationWaitTime = maxOperationWaitTime;
        this.store = store;
        this.rowKey = string2StaticBuffer(identifier);
        this.serializer = new StandardSerializer();
    }

    /**
     * Reads the configuration property for this StoreManager
     *
     * @param key Key identifying the configuration property
     * @return Value stored for the key or null if the configuration property has not (yet) been defined.
     */
    @Override
    public <O> O get(String key, Class<O> dataType) {
        StaticBuffer column = string2StaticBuffer(key);
        KeySliceQuery query = new KeySliceQuery(rowKey, column, BufferUtil.nextBiggerBuffer(column));
        StaticBuffer result = BackendOperation.execute(new BackendOperation.Transactional<StaticBuffer>() {
            @Override
            public StaticBuffer call(StoreTransaction txh) throws BackendException {
                List<Entry> entries = store.getSlice(query, txh);
                if (entries.isEmpty()) return null;
                return entries.get(0).getValueAs(StaticBuffer.STATIC_FACTORY);
            }

            @Override
            public String toString() {
                return "getConfiguration";
            }
        }, txProvider, times, maxOperationWaitTime);
        if (result == null) return null;
        return staticBuffer2Object(result, dataType);
    }

    /**
     * Sets a configuration property for this StoreManager.
     *
     * @param key   Key identifying the configuration property
     * @param value Value to be stored for the key
     */
    @Override
    public <O> void set(String key, O value) {
        StaticBuffer column = string2StaticBuffer(key);
        List<Entry> additions;
        List<StaticBuffer> deletions;
        if (value != null) { //Addition
            additions = new ArrayList<>(1);
            deletions = KeyColumnValueStore.NO_DELETIONS;
            StaticBuffer val = object2StaticBuffer(value);
            additions.add(StaticArrayEntry.of(column, val));
        } else { //Deletion
            additions = KeyColumnValueStore.NO_ADDITIONS;
            deletions = Lists.newArrayList(column);
        }

        BackendOperation.execute(new BackendOperation.Transactional<Boolean>() {
            @Override
            public Boolean call(StoreTransaction txh) throws BackendException {
                store.mutate(rowKey, additions, deletions, txh);
                return true;
            }

            @Override
            public String toString() {
                return "setConfiguration";
            }
        }, txProvider, times, maxOperationWaitTime);
    }

    @Override
    public void remove(String key) {
        set(key, null);
    }

    public ReadConfiguration asReadConfiguration() {
        Map<String, Object> entries = toMap();
        return new ReadConfiguration() {
            @Override
            public <O> O get(String key, Class<O> dataType) {
                Preconditions.checkArgument(!entries.containsKey(key) || dataType.isAssignableFrom(entries.get(key).getClass()));
                return (O) entries.get(key);
            }

            @Override
            public Iterable<String> getKeys(String prefix) {
                final boolean prefixBlank = StringUtils.isBlank(prefix);
                return entries.keySet().stream().filter(s -> prefixBlank || s.startsWith(prefix)).collect(Collectors.toList());
            }

            @Override
            public void close() {
                //Do nothing
            }
        };
    }

    @Override
    public Iterable<String> getKeys(String prefix) {
        return asReadConfiguration().getKeys(prefix);
    }

    @Override
    public void close() {
        try {
            store.close();
            txProvider.close();
        } catch (BackendException e) {
            throw new JanusGraphException("Could not close configuration store", e);
        }
    }


    private Map<String, Object> toMap() {
        Map<String, Object> entries = Maps.newHashMap();
        List<Entry> result = BackendOperation.execute(new BackendOperation.Transactional<List<Entry>>() {
            @Override
            public List<Entry> call(StoreTransaction txh) throws BackendException {
                return store.getSlice(new KeySliceQuery(rowKey, BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)), txh);
            }

            @Override
            public String toString() {
                return "setConfiguration";
            }
        }, txProvider, times, maxOperationWaitTime);

        for (Entry entry : result) {
            String key = staticBuffer2String(entry.getColumnAs(StaticBuffer.STATIC_FACTORY));
            Object value = staticBuffer2Object(entry.getValueAs(StaticBuffer.STATIC_FACTORY), Object.class);
            entries.put(key, value);
        }
        return entries;
    }


    private StaticBuffer string2StaticBuffer(String s) {
        ByteBuffer out = ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
        return StaticArrayBuffer.of(out);
    }

    private String staticBuffer2String(StaticBuffer s) {
        return new String(s.as(StaticBuffer.ARRAY_FACTORY), StandardCharsets.UTF_8);
    }

    private <O> StaticBuffer object2StaticBuffer(O value) {
        if (value == null) {
            throw Graph.Variables.Exceptions.variableValueCanNotBeNull();
        }
        if (!serializer.validDataType(value.getClass())) {
            throw Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(value);
        }
        DataOutput out = serializer.getDataOutput(128);
        out.writeClassAndObject(value);
        return out.getStaticBuffer();
    }

    private <O> O staticBuffer2Object(StaticBuffer s, Class<O> dataType) {
        Object value = serializer.readClassAndObject(s.asReadBuffer());
        Preconditions.checkArgument(dataType.isInstance(value), "Could not deserialize to [%s], got: %s", dataType, value);
        return (O) value;
    }

}
