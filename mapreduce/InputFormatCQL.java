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

package grakn.core.mapreduce;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.configuration.BasicConfiguration;
import grakn.core.graph.diskstorage.cql.CQLConfigOptions;
import grakn.core.graph.diskstorage.cql.CQLKeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.diskstorage.util.StaticArrayBuffer;
import grakn.core.graph.diskstorage.util.StaticArrayEntry;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.HadoopPoolsConfigurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InputFormatCQL extends InputFormat<StaticBuffer, Iterable<Entry>> implements HadoopPoolsConfigurable {

    private static final Logger LOG = LoggerFactory.getLogger(InputFormatCQL.class);

    // Copied these private constants from Cassandra's ConfigHelper circa 2.0.9
    private static final String INPUT_WIDEROWS_CONFIG = "cassandra.input.widerows";
    private static final String RANGE_BATCH_SIZE_CONFIG = "cassandra.range.batch.size";
    private static final StaticBuffer DEFAULT_COLUMN = StaticArrayBuffer.of(new byte[0]);
    private static final SliceQuery DEFAULT_SLICE_QUERY = new SliceQuery(DEFAULT_COLUMN, DEFAULT_COLUMN);
    private static final String INPUT_NATIVE_AUTH_PROVIDER = "cassandra.input.native.auth.provider";
    private static final String USERNAME = "cassandra.username";
    private static final String PASSWORD = "cassandra.password";

    private final InputFormatGrakn cqlInputFormat = new InputFormatGrakn();
    private Configuration hadoopConf;

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
        return cqlInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<StaticBuffer, Iterable<Entry>> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        InputFormatGrakn.RecordReaderGrakn recordReader = (InputFormatGrakn.RecordReaderGrakn) cqlInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        return new RecordReaderCQL(recordReader);
    }

    @Override
    public void setConf(Configuration config) {
        this.hadoopConf = config;
        HadoopPoolsConfigurable.super.setConf(config);
        ModifiableConfigurationHadoop mrConf = ModifiableConfigurationHadoop.of(ModifiableConfigurationHadoop.MAPRED_NS, config);
        BasicConfiguration janusgraphConf = mrConf.getJanusGraphConf();

        // Copy some JanusGraph configuration keys to the Hadoop Configuration keys used by Cassandra's ColumnFamilyInputFormat
        ConfigHelper.setInputInitialAddress(config, janusgraphConf.get(GraphDatabaseConfiguration.STORAGE_HOSTS)[0]);
        if (janusgraphConf.has(GraphDatabaseConfiguration.STORAGE_PORT)) {
            ConfigHelper.setInputRpcPort(config, String.valueOf(janusgraphConf.get(GraphDatabaseConfiguration.STORAGE_PORT)));
        }
        if (janusgraphConf.has(GraphDatabaseConfiguration.AUTH_USERNAME) && janusgraphConf.has(GraphDatabaseConfiguration.AUTH_PASSWORD)) {
            String username = janusgraphConf.get(GraphDatabaseConfiguration.AUTH_PASSWORD);
            if (StringUtils.isNotBlank(username)) {
                config.set(INPUT_NATIVE_AUTH_PROVIDER, PlainTextAuthProvider.class.getName());
                config.set(USERNAME, username);
                config.set(PASSWORD, janusgraphConf.get(GraphDatabaseConfiguration.AUTH_USERNAME));
            }
        }
        // Copy keyspace, force the CF setting to edgestore, honor widerows when set
        boolean wideRows = config.getBoolean(INPUT_WIDEROWS_CONFIG, false);
        // Use the setInputColumnFamily overload that includes a widerows argument; using the overload without this argument forces it false
        ConfigHelper.setInputColumnFamily(config, janusgraphConf.get(CQLConfigOptions.KEYSPACE),
                                          mrConf.get(ModifiableConfigurationHadoop.COLUMN_FAMILY_NAME), wideRows);
        LOG.debug("Set keyspace: {}", janusgraphConf.get(CQLConfigOptions.KEYSPACE));

        // Set the column slice bounds via Faunus' vertex query filter
        SlicePredicate predicate = new SlicePredicate();
        int rangeBatchSize = config.getInt(RANGE_BATCH_SIZE_CONFIG, Integer.MAX_VALUE);
        predicate.setSlice_range(getSliceRange(rangeBatchSize)); // TODO stop slicing the whole row
        ConfigHelper.setInputSlicePredicate(config, predicate);
    }

    private SliceRange getSliceRange(int limit) {
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(DEFAULT_SLICE_QUERY.getSliceStart().asByteBuffer());
        sliceRange.setFinish(DEFAULT_SLICE_QUERY.getSliceEnd().asByteBuffer());
        sliceRange.setCount(Math.min(limit, DEFAULT_SLICE_QUERY.getLimit()));
        return sliceRange;
    }

    @Override
    public Configuration getConf() {
        return hadoopConf;
    }

    private static class RecordReaderCQL extends RecordReader<StaticBuffer, Iterable<Entry>> {
        private KV currentKV;
        private KV incompleteKV;

        private final InputFormatGrakn.RecordReaderGrakn reader;

        RecordReaderCQL(InputFormatGrakn.RecordReaderGrakn reader) {
            this.reader = reader;
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
            reader.initialize(inputSplit, taskAttemptContext);
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            return null != (currentKV = completeNextKV());
        }

        private KV completeNextKV() throws IOException {
            KV completedKV = null;
            boolean hasNext;
            do {
                hasNext = reader.nextKeyValue();

                if (!hasNext) {
                    completedKV = incompleteKV;
                    incompleteKV = null;
                } else {
                    Row row = reader.getCurrentValue();
                    StaticArrayBuffer key = StaticArrayBuffer.of(row.getBytesUnsafe(CQLKeyColumnValueStore.KEY_COLUMN_NAME));
                    StaticBuffer column1 = StaticArrayBuffer.of(row.getBytesUnsafe(CQLKeyColumnValueStore.COLUMN_COLUMN_NAME));
                    StaticBuffer value = StaticArrayBuffer.of(row.getBytesUnsafe(CQLKeyColumnValueStore.VALUE_COLUMN_NAME));
                    Entry entry = StaticArrayEntry.of(column1, value);

                    if (null == incompleteKV) {
                        // Initialization; this should happen just once in an instance's lifetime
                        incompleteKV = new KV(key);
                    } else if (!incompleteKV.key.equals(key)) {
                        // The underlying Cassandra reader has just changed to a key we haven't seen yet
                        // This implies that there will be no more entries for the prior key
                        completedKV = incompleteKV;
                        incompleteKV = new KV(key);
                    }

                    incompleteKV.addEntry(entry);
                }
                /* Loop ends when either
                 * A) the cassandra reader ran out of data
                 * or
                 * B) the cassandra reader switched keys, thereby completing a KV */
            } while (hasNext && null == completedKV);

            return completedKV;
        }

        @Override
        public StaticBuffer getCurrentKey() {
            return currentKV.key;
        }

        @Override
        public Iterable<Entry> getCurrentValue() {
            return currentKV.entries;
        }

        @Override
        public void close() {
            reader.close();
        }

        @Override
        public float getProgress() {
            return reader.getProgress();
        }

        private static class KV {
            private final StaticArrayBuffer key;
            private final ArrayList<Entry> entries = new ArrayList<>();

            KV(StaticArrayBuffer key) {
                this.key = key;
            }

            void addEntry(Entry toAdd) {
                entries.add(toAdd);
            }
        }
    }
}
