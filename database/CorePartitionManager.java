/*
 * Copyright (C) 2022 Vaticle
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

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.graph.common.Storage.Key;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.graph.common.Storage.Key.Partition.DEFAULT;
import static com.vaticle.typedb.core.graph.common.Storage.Key.Partition.FIXED_START_EDGE;
import static com.vaticle.typedb.core.graph.common.Storage.Key.Partition.METADATA;
import static com.vaticle.typedb.core.graph.common.Storage.Key.Partition.OPTIMISATION_EDGE;
import static com.vaticle.typedb.core.graph.common.Storage.Key.Partition.VARIABLE_START_EDGE;

public abstract class CorePartitionManager {

    private final List<ColumnFamilyDescriptor> descriptors;
    final List<ColumnFamilyHandle> handles;

    private CorePartitionManager(List<ColumnFamilyDescriptor> descriptors, List<ColumnFamilyHandle> handles) {
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

    abstract Set<Key.Partition> partitions();

    protected void close() {
        descriptors.forEach(descriptor -> descriptor.getOptions().close());
        handles.forEach(AbstractImmutableNativeReference::close);
    }

    public static class Schema extends CorePartitionManager {

        protected final ColumnFamilyHandle defaultHandle;

        protected Schema(List<ColumnFamilyDescriptor> descriptors, List<ColumnFamilyHandle> handles) {
            super(descriptors, handles);
            defaultHandle = handles.get(0);
        }

        static List<ColumnFamilyDescriptor> descriptors(RocksConfiguration.Schema configuration) {
            return list(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, configuration.defaultCFOptions()));
        }

        @Override
        public ColumnFamilyHandle get(Key.Partition partition) {
            if (partition == DEFAULT) return defaultHandle;
            else throw TypeDBException.of(ILLEGAL_STATE);
        }

        @Override
        Set<Key.Partition> partitions() {
            return set(DEFAULT);
        }
    }

    public static class Data extends CorePartitionManager {

        private static final int DEFAULT_HANDLE_INDEX = 0;
        private static final int VARIABLE_START_EDGE_HANDLE_INDEX = 1;
        private static final int FIXED_START_EDGE_HANDLE_INDEX = 2;
        private static final int OPTIMISATION_EDGE_HANDLE_INDEX = 3;
        private static final int METADATA_HANDLE_INDEX = 4;

        protected final ColumnFamilyHandle defaultHandle;
        protected final ColumnFamilyHandle variableStartEdgeHandle;
        protected final ColumnFamilyHandle fixedStartEdgeHandle;
        protected final ColumnFamilyHandle optimisationEdgeHandle;
        protected final ColumnFamilyHandle metadataHandle;

        protected Data(List<ColumnFamilyDescriptor> descriptors, List<ColumnFamilyHandle> handles) {
            super(descriptors, handles);
            defaultHandle = handles.get(DEFAULT_HANDLE_INDEX);
            variableStartEdgeHandle = handles.get(VARIABLE_START_EDGE_HANDLE_INDEX);
            fixedStartEdgeHandle = handles.get(FIXED_START_EDGE_HANDLE_INDEX);
            optimisationEdgeHandle = handles.get(OPTIMISATION_EDGE_HANDLE_INDEX);
            metadataHandle = handles.get(METADATA_HANDLE_INDEX);
        }

        static List<ColumnFamilyDescriptor> descriptors(RocksConfiguration.Data configuration) {
            ColumnFamilyDescriptor[] descriptors = new ColumnFamilyDescriptor[5];
            descriptors[DEFAULT_HANDLE_INDEX] = new ColumnFamilyDescriptor(
                    RocksDB.DEFAULT_COLUMN_FAMILY,
                    configuration.defaultCFOptions()
            );
            descriptors[VARIABLE_START_EDGE_HANDLE_INDEX] = new ColumnFamilyDescriptor(
                    new byte[]{VARIABLE_START_EDGE.encoding().ID()},
                    configuration.variableStartEdgeCFOptions()
            );
            descriptors[FIXED_START_EDGE_HANDLE_INDEX] = new ColumnFamilyDescriptor(
                    new byte[]{FIXED_START_EDGE.encoding().ID()},
                    configuration.fixedStartEdgeCFOptions()
            );
            descriptors[OPTIMISATION_EDGE_HANDLE_INDEX] = new ColumnFamilyDescriptor(
                    new byte[]{OPTIMISATION_EDGE.encoding().ID()},
                    configuration.optimisationEdgeCFOptions()
            );
            descriptors[METADATA_HANDLE_INDEX] = new ColumnFamilyDescriptor(
                    new byte[]{METADATA.encoding().ID()},
                    configuration.metadataCFOptions()
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
                case METADATA:
                    return metadataHandle;
                default:
                    throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        @Override
        Set<Key.Partition> partitions() {
            return set(DEFAULT, VARIABLE_START_EDGE, FIXED_START_EDGE, OPTIMISATION_EDGE, METADATA);
        }
    }
}
