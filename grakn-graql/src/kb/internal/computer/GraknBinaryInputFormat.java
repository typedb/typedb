/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
package grakn.core.kb.internal.computer;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import org.janusgraph.hadoop.formats.util.AbstractBinaryInputFormat;
import org.janusgraph.hadoop.formats.util.input.JanusGraphHadoopSetupCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Override JanusGraph's Cassandra3BinaryInputFormat class
 *
 * This class removes dependency from columnFamilyInput (which does not exist in Cassandra 3.x)
 * and instead it relies on the newer CqlInputFormat.
 */
public class GraknBinaryInputFormat extends AbstractBinaryInputFormat {
    private static final Logger log = LoggerFactory.getLogger(GraknBinaryInputFormat.class);
    private static final String INPUT_WIDEROWS_CONFIG = "cassandra.input.widerows";
    private static final String RANGE_BATCH_SIZE_CONFIG = "cassandra.range.batch.size";
    private final CqlInputFormat cqlInputFormat = new CqlInputFormat();
    RecordReader<StaticBuffer, Iterable<Entry>> janusgraphRecordReader;

    public GraknBinaryInputFormat() {
    }

    public RecordReader<StaticBuffer, Iterable<Entry>> getRecordReader() {
        return this.janusgraphRecordReader;
    }

    public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
        return this.cqlInputFormat.getSplits(jobContext);
    }

    public RecordReader<StaticBuffer, Iterable<Entry>> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.janusgraphRecordReader = new GraknCqlBridgeRecordReader();
        return this.janusgraphRecordReader;
    }

    @Override
    public void setConf(final Configuration config) {
        super.setConf(config);

        // Copy some JanusGraph configuration keys to the Hadoop Configuration keys used by Cassandra's ColumnFamilyInputFormat
        ConfigHelper.setInputInitialAddress(config, janusgraphConf.get(GraphDatabaseConfiguration.STORAGE_HOSTS)[0]);
        if (janusgraphConf.has(GraphDatabaseConfiguration.STORAGE_PORT)){
            ConfigHelper.setInputRpcPort(config, String.valueOf(janusgraphConf.get(GraphDatabaseConfiguration.STORAGE_PORT)));
        }
        if (janusgraphConf.has(GraphDatabaseConfiguration.AUTH_USERNAME)){
            ConfigHelper.setInputKeyspaceUserName(config, janusgraphConf.get(GraphDatabaseConfiguration.AUTH_USERNAME));
        }
        if (janusgraphConf.has(GraphDatabaseConfiguration.AUTH_PASSWORD)) {
            ConfigHelper.setInputKeyspacePassword(config, janusgraphConf.get(GraphDatabaseConfiguration.AUTH_PASSWORD));
        }
        // Copy keyspace, force the CF setting to edgestore, honor widerows when set
        final boolean wideRows = config.getBoolean(INPUT_WIDEROWS_CONFIG, false);
        // Use the setInputColumnFamily overload that includes a widerows argument; using the overload without this argument forces it false
        ConfigHelper.setInputColumnFamily(config, janusgraphConf.get(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE),
                mrConf.get(JanusGraphHadoopConfiguration.COLUMN_FAMILY_NAME), wideRows);
        log.debug("Set keyspace: {}", janusgraphConf.get(AbstractCassandraStoreManager.CASSANDRA_KEYSPACE));

        // Set the column slice bounds via Faunus' vertex query filter
        final SlicePredicate predicate = new SlicePredicate();
        final int rangeBatchSize = config.getInt(RANGE_BATCH_SIZE_CONFIG, Integer.MAX_VALUE);
        predicate.setSlice_range(getSliceRange(rangeBatchSize)); // TODO stop slicing the whole row
        ConfigHelper.setInputSlicePredicate(config, predicate);
    }

    private SliceRange getSliceRange(final int limit) {
        final SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(JanusGraphHadoopSetupCommon.DEFAULT_SLICE_QUERY.getSliceStart().asByteBuffer());
        sliceRange.setFinish(JanusGraphHadoopSetupCommon.DEFAULT_SLICE_QUERY.getSliceEnd().asByteBuffer());
        sliceRange.setCount(Math.min(limit, JanusGraphHadoopSetupCommon.DEFAULT_SLICE_QUERY.getLimit()));
        return sliceRange;
    }
}
