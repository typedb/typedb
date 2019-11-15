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

package grakn.core.graph.hadoop.formats.cql;

import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.cql.CQLConfigOptions;
import grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration;
import grakn.core.graph.hadoop.config.JanusGraphHadoopConfiguration;
import grakn.core.graph.hadoop.formats.util.AbstractBinaryInputFormat;
import grakn.core.graph.hadoop.formats.util.input.JanusGraphHadoopSetupImpl;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class CqlBinaryInputFormat extends AbstractBinaryInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(CqlBinaryInputFormat.class);

    // Copied these private constants from Cassandra's ConfigHelper circa 2.0.9
    private static final String INPUT_WIDEROWS_CONFIG = "cassandra.input.widerows";
    private static final String RANGE_BATCH_SIZE_CONFIG = "cassandra.range.batch.size";

    private final GraknInputFormat cqlInputFormat = new GraknInputFormat();

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
        return cqlInputFormat.getSplits(jobContext);
    }

    @Override
    public RecordReader<StaticBuffer, Iterable<Entry>> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        GraknCqlRecordReader recordReader = (GraknCqlRecordReader) cqlInputFormat.createRecordReader(inputSplit, taskAttemptContext);
        return new CqlBinaryRecordReader(recordReader);
    }

    @Override
    public void setConf(Configuration config) {
        super.setConf(config);

        // Copy some JanusGraph configuration keys to the Hadoop Configuration keys used by Cassandra's ColumnFamilyInputFormat
        ConfigHelper.setInputInitialAddress(config, janusgraphConf.get(GraphDatabaseConfiguration.STORAGE_HOSTS)[0]);
        if (janusgraphConf.has(GraphDatabaseConfiguration.STORAGE_PORT)) {
            ConfigHelper.setInputRpcPort(config, String.valueOf(janusgraphConf.get(GraphDatabaseConfiguration.STORAGE_PORT)));
        }
        if (janusgraphConf.has(GraphDatabaseConfiguration.AUTH_USERNAME) && janusgraphConf.has(GraphDatabaseConfiguration.AUTH_PASSWORD)) {
            GraknCqlConfigHelper.setUserNameAndPassword(config,
                    janusgraphConf.get(GraphDatabaseConfiguration.AUTH_PASSWORD),
                    janusgraphConf.get(GraphDatabaseConfiguration.AUTH_USERNAME));
        }
        // Copy keyspace, force the CF setting to edgestore, honor widerows when set
        boolean wideRows = config.getBoolean(INPUT_WIDEROWS_CONFIG, false);
        // Use the setInputColumnFamily overload that includes a widerows argument; using the overload without this argument forces it false
        ConfigHelper.setInputColumnFamily(config, janusgraphConf.get(CQLConfigOptions.KEYSPACE),
                mrConf.get(JanusGraphHadoopConfiguration.COLUMN_FAMILY_NAME), wideRows);
        LOG.debug("Set keyspace: {}", janusgraphConf.get(CQLConfigOptions.KEYSPACE));

        // Set the column slice bounds via Faunus' vertex query filter
        SlicePredicate predicate = new SlicePredicate();
        int rangeBatchSize = config.getInt(RANGE_BATCH_SIZE_CONFIG, Integer.MAX_VALUE);
        predicate.setSlice_range(getSliceRange(rangeBatchSize)); // TODO stop slicing the whole row
        ConfigHelper.setInputSlicePredicate(config, predicate);
    }

    private SliceRange getSliceRange(int limit) {
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(JanusGraphHadoopSetupImpl.DEFAULT_SLICE_QUERY.getSliceStart().asByteBuffer());
        sliceRange.setFinish(JanusGraphHadoopSetupImpl.DEFAULT_SLICE_QUERY.getSliceEnd().asByteBuffer());
        sliceRange.setCount(Math.min(limit, JanusGraphHadoopSetupImpl.DEFAULT_SLICE_QUERY.getLimit()));
        return sliceRange;
    }
}
