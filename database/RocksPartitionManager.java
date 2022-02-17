/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.database;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.common.Storage.Key;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.graph.common.Encoding.ValueType.STRING_ENCODING;
import static com.vaticle.typedb.core.graph.common.Storage.Key.Partition.DEFAULT;
import static com.vaticle.typedb.core.graph.common.Storage.Key.Partition.FIXED_START_EDGE;
import static com.vaticle.typedb.core.graph.common.Storage.Key.Partition.OPTIMISATION_EDGE;
import static com.vaticle.typedb.core.graph.common.Storage.Key.Partition.STATISTICS;
import static com.vaticle.typedb.core.graph.common.Storage.Key.Partition.VARIABLE_START_EDGE;

public abstract class RocksPartitionManager {

    private final List<ColumnFamilyDescriptor> descriptors;
    final List<ColumnFamilyHandle> handles;

    private RocksPartitionManager(List<ColumnFamilyDescriptor> descriptors, List<ColumnFamilyHandle> handles) {
        validateListsMatch(descriptors, handles);
        this.descriptors = descriptors;
        this.handles = handles;
    }

    private void validateListsMatch(List<ColumnFamilyDescriptor> descriptors, List<ColumnFamilyHandle> handles) {
        assert descriptors.size() == handles.size() && IntStream.range(0, descriptors.size()).allMatch(i -> {
            try {
                return Arrays.equals(descriptors.get(i).getName(), handles.get(i).getDescriptor().getName());
            } catch (RocksDBException e) {
                throw TypeDBException.of(e);
            }
        });
    }

    public abstract ColumnFamilyHandle get(Key.Partition partition);

    public abstract Key.Partition partition(int columnFamilyID);

    abstract Set<Key.Partition> partitions();

    void close() {
        descriptors.forEach(descriptor -> descriptor.getOptions().close());
        handles.forEach(AbstractImmutableNativeReference::close);
    }

    static class Schema extends RocksPartitionManager {

        private final ColumnFamilyHandle defaultHandle;
        private final Map<Integer, Key.Partition> partitionMap;

        Schema(List<ColumnFamilyDescriptor> descriptors, List<ColumnFamilyHandle> handles) {
            super(descriptors, handles);
            defaultHandle = handles.get(0);
            partitionMap = map(pair(defaultHandle.getID(), DEFAULT));
        }

        static List<ColumnFamilyDescriptor> descriptors(RocksConfiguration.Schema configuration) {
            return list(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, configuration.defaultCFOptions()));
        }

        @Override
        public ColumnFamilyHandle get(Key.Partition partition) {
            if (partition == DEFAULT) return defaultHandle;
            else throw TypeDBException.of(ILLEGAL_STATE);
        }

        public Key.Partition partition(int columnFamilyID) {
            Key.Partition partition = partitionMap.get(columnFamilyID);
            if (partition == null) throw TypeDBException.of(ILLEGAL_STATE);
            if (partition != DEFAULT) throw TypeDBException.of(ILLEGAL_STATE);
            return partition;
        }

        @Override
        Set<Key.Partition> partitions() {
            return set(DEFAULT);
        }
    }

    public static class Data extends RocksPartitionManager {

        private static final int DEFAULT_HANDLE_INDEX = 0;
        private static final int VARIABLE_START_EDGE_HANDLE_INDEX = 1;
        private static final int FIXED_START_EDGE_HANDLE_INDEX = 2;
        private static final int OPTIMISATION_EDGE_HANDLE_INDEX = 3;
        private static final int STATISTICS_HANDLE_INDEX = 4;

        private final Map<Integer, Key.Partition> partitionMap;
        private final ColumnFamilyHandle defaultHandle;
        private final ColumnFamilyHandle variableStartEdgeHandle;
        private final ColumnFamilyHandle fixedStartEdgeHandle;
        private final ColumnFamilyHandle optimisationEdgeHandle;
        private final ColumnFamilyHandle statisticsHandle;

        Data(List<ColumnFamilyDescriptor> descriptors, List<ColumnFamilyHandle> handles) {
            super(descriptors, handles);
            defaultHandle = handles.get(DEFAULT_HANDLE_INDEX);
            variableStartEdgeHandle = handles.get(VARIABLE_START_EDGE_HANDLE_INDEX);
            fixedStartEdgeHandle = handles.get(FIXED_START_EDGE_HANDLE_INDEX);
            optimisationEdgeHandle = handles.get(OPTIMISATION_EDGE_HANDLE_INDEX);
            statisticsHandle = handles.get(STATISTICS_HANDLE_INDEX);
            partitionMap = map(
                    pair(defaultHandle.getID(), DEFAULT),
                    pair(variableStartEdgeHandle.getID(), VARIABLE_START_EDGE),
                    pair(fixedStartEdgeHandle.getID(), FIXED_START_EDGE),
                    pair(optimisationEdgeHandle.getID(), OPTIMISATION_EDGE),
                    pair(statisticsHandle.getID(), STATISTICS)
            );
        }

        static List<ColumnFamilyDescriptor> descriptors(RocksConfiguration.Data configuration) {
            ColumnFamilyDescriptor[] descriptors = new ColumnFamilyDescriptor[5];
            descriptors[DEFAULT_HANDLE_INDEX] = new ColumnFamilyDescriptor(
                    RocksDB.DEFAULT_COLUMN_FAMILY,
                    configuration.defaultCFOptions()
            );
            descriptors[VARIABLE_START_EDGE_HANDLE_INDEX] = new ColumnFamilyDescriptor(
                    ByteArray.encodeString(VARIABLE_START_EDGE.name(), STRING_ENCODING).getBytes(),
                    configuration.variableStartEdgeCFOptions()
            );
            descriptors[FIXED_START_EDGE_HANDLE_INDEX] = new ColumnFamilyDescriptor(
                    ByteArray.encodeString(FIXED_START_EDGE.name(), STRING_ENCODING).getBytes(),
                    configuration.fixedStartEdgeCFOptions()
            );
            descriptors[OPTIMISATION_EDGE_HANDLE_INDEX] = new ColumnFamilyDescriptor(
                    ByteArray.encodeString(OPTIMISATION_EDGE.name(), STRING_ENCODING).getBytes(),
                    configuration.optimisationEdgeCFOptions()
            );
            descriptors[STATISTICS_HANDLE_INDEX] = new ColumnFamilyDescriptor(
                    ByteArray.encodeString(STATISTICS.name(), STRING_ENCODING).getBytes(),
                    configuration.statisticsCFOptions()
            );
            return Arrays.asList(descriptors);
        }

        @Override
        public ColumnFamilyHandle get(Key.Partition partition) {
            switch (partition) {
                case DEFAULT:
                    return defaultHandle;
                case VARIABLE_START_EDGE:
                    return variableStartEdgeHandle;
                case FIXED_START_EDGE:
                    return fixedStartEdgeHandle;
                case OPTIMISATION_EDGE:
                    return optimisationEdgeHandle;
                case STATISTICS:
                    return statisticsHandle;
                default:
                    throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        public Key.Partition partition(int columnFamilyID) {
            Key.Partition partition = partitionMap.get(columnFamilyID);
            if (partition == null) throw TypeDBException.of(ILLEGAL_STATE);
            return partition;
        }

        @Override
        Set<Key.Partition> partitions() {
            return set(DEFAULT, VARIABLE_START_EDGE, FIXED_START_EDGE, OPTIMISATION_EDGE, STATISTICS);
        }
    }
}
