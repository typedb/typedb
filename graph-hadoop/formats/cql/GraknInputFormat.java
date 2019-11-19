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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenRange;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.hadoop.ReporterWrapper;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static grakn.core.common.config.ConfigKey.STORAGE_PORT;
import static java.util.stream.Collectors.toMap;

/**
 * Hadoop InputFormat allowing map/reduce against Cassandra rows within one ColumnFamily.
 * <p>
 * At minimum, you need to set the KS and CF in your Hadoop job Configuration.
 * The ConfigHelper class is provided to make this
 * simple:
 * ConfigHelper.setInputColumnFamily
 * <p>
 * You can also configure the number of rows per InputSplit with
 * 1: ConfigHelper.setInputSplitSize. The default split size is 64k rows.
 * or
 * 2: ConfigHelper.setInputSplitSizeInMb. InputSplit size in MB with new, more precise method
 * If no value is provided for InputSplitSizeInMb, we default to using InputSplitSize.
 * <p>
 * CQLConfigHelper.setInputCQLPageRowSize. The default page row size is 1000. You
 * should set it to "as big as possible, but no bigger." It set the LIMIT for the CQL
 * query, so you need set it big enough to minimize the network overhead, and also
 * not too big to avoid out of memory issue.
 * <p>
 * other native protocol connection parameters in CqlConfigHelper
 */

public class GraknInputFormat extends org.apache.hadoop.mapreduce.InputFormat<Long, Row> implements org.apache.hadoop.mapred.InputFormat<Long, Row> {
    public static final String MAPRED_TASK_ID = "mapred.task.id";
    private static final Logger LOG = LoggerFactory.getLogger(CqlInputFormat.class);

    private String keyspace;
    private String cfName;
    private IPartitioner partitioner;

    public RecordReader<Long, Row> getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter) throws IOException {
        TaskAttemptContext tac = HadoopCompat.newMapContext(
                jobConf,
                TaskAttemptID.forName(jobConf.get(MAPRED_TASK_ID)),
                null,
                null,
                null,
                new ReporterWrapper(reporter),
                null);


        GraknCqlRecordReader recordReader = new GraknCqlRecordReader();
        recordReader.initialize((org.apache.hadoop.mapreduce.InputSplit) split, tac);
        return recordReader;
    }

    @Override
    public org.apache.hadoop.mapreduce.RecordReader<Long, Row> createRecordReader(org.apache.hadoop.mapreduce.InputSplit arg0, TaskAttemptContext arg1) {
        return new GraknCqlRecordReader();
    }

    protected void validateConfiguration(Configuration conf) {
        if (ConfigHelper.getInputKeyspace(conf) == null || ConfigHelper.getInputColumnFamily(conf) == null) {
            throw new UnsupportedOperationException("you must set the keyspace and table with setInputColumnFamily()");
        }
        if (ConfigHelper.getInputInitialAddress(conf) == null) {
            throw new UnsupportedOperationException("You must set the initial output address to a Cassandra node with setInputInitialAddress");
        }
        if (ConfigHelper.getInputPartitioner(conf) == null) {
            throw new UnsupportedOperationException("You must set the Cassandra partitioner class with setInputPartitioner");
        }
    }

    public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext context) throws IOException {
        Configuration conf = HadoopCompat.getConfiguration(context);

        validateConfiguration(conf);

        keyspace = ConfigHelper.getInputKeyspace(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        partitioner = ConfigHelper.getInputPartitioner(conf);
        LOG.trace("partitioner is {}", partitioner);

        // canonical ranges, split into pieces, fetching the splits in parallel
        ExecutorService executor = new ThreadPoolExecutor(0, 128, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        List<org.apache.hadoop.mapreduce.InputSplit> splits = new ArrayList<>();

        try (CqlSession session = getInputSession(ConfigHelper.getInputInitialAddress(conf).split(","), conf)) {
            List<Future<List<org.apache.hadoop.mapreduce.InputSplit>>> splitfutures = new ArrayList<>();
            KeyRange jobKeyRange = ConfigHelper.getInputKeyRange(conf);
            Range<Token> jobRange = null;
            if (jobKeyRange != null) {
                if (jobKeyRange.start_key != null) {
                    if (!partitioner.preservesOrder()) {
                        throw new UnsupportedOperationException("KeyRange based on keys can only be used with a order preserving partitioner");
                    }
                    if (jobKeyRange.start_token != null) {
                        throw new IllegalArgumentException("only start_key supported");
                    }
                    if (jobKeyRange.end_token != null) {
                        throw new IllegalArgumentException("only start_key supported");
                    }
                    jobRange = new Range<>(partitioner.getToken(jobKeyRange.start_key),
                            partitioner.getToken(jobKeyRange.end_key));
                } else if (jobKeyRange.start_token != null) {
                    jobRange = new Range<>(partitioner.getTokenFactory().fromString(jobKeyRange.start_token),
                            partitioner.getTokenFactory().fromString(jobKeyRange.end_token));
                } else {
                    LOG.warn("ignoring jobKeyRange specified without start_key or start_token");
                }
            }

            Metadata metadata = session.getMetadata();

            // canonical ranges and nodes holding replicas
            Map<TokenRange, Set<Node>> masterRangeNodes = getRangeMap(keyspace, metadata);

            for (TokenRange range : masterRangeNodes.keySet()) {
                if (jobRange == null) {
                    // for each tokenRange, pick a live owner and ask it to compute bite-sized splits
                    splitfutures.add(executor.submit(new SplitCallable(range, masterRangeNodes.get(range), conf, session)));
                } else {
                    TokenRange jobTokenRange = rangeToTokenRange(metadata, jobRange);
                    if (range.intersects(jobTokenRange)) {
                        for (TokenRange intersection : range.intersectWith(jobTokenRange)) {
                            // for each tokenRange, pick a live owner and ask it to compute bite-sized splits
                            splitfutures.add(executor.submit(new SplitCallable(intersection, masterRangeNodes.get(range), conf, session)));
                        }
                    }
                }
            }

            // wait until we have all the results back
            for (Future<List<org.apache.hadoop.mapreduce.InputSplit>> futureInputSplits : splitfutures) {
                try {
                    splits.addAll(futureInputSplits.get());
                } catch (Exception e) {
                    throw new IOException("Could not get input splits", e);
                }
            }
        } finally {
            executor.shutdownNow();
        }

        Collections.shuffle(splits, new Random(System.nanoTime()));
        return splits;
    }

    private TokenRange rangeToTokenRange(Metadata metadata, Range<Token> range) {
        TokenMap tokenMap = metadata.getTokenMap().get();
        return tokenMap.newTokenRange(tokenMap.parse(partitioner.getTokenFactory().toString(range.left)),
                tokenMap.parse(partitioner.getTokenFactory().toString(range.right)));
    }

    private Map<TokenRange, Long> getSubSplits(String keyspace, String cfName, TokenRange range, Configuration conf, CqlSession session) {
        int splitSize = ConfigHelper.getInputSplitSize(conf);
        int splitSizeMb = ConfigHelper.getInputSplitSizeInMb(conf);
        try {
            return describeSplits(keyspace, cfName, range, splitSize, splitSizeMb, session);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<TokenRange, Set<Node>> getRangeMap(String keyspace, Metadata metadata) {
        return metadata.getTokenMap().get().getTokenRanges().stream()
                .collect(toMap(p -> p, p -> metadata.getTokenMap().get().getReplicas('"' + keyspace + '"', p)));
    }

    private Map<TokenRange, Long> describeSplits(String keyspace, String table, TokenRange tokenRange, int splitSize, int splitSizeMb, CqlSession session) {
        String query = String.format("SELECT mean_partition_size, partitions_count " +
                        "FROM %s.%s " +
                        "WHERE keyspace_name = ? AND table_name = ? AND range_start = ? AND range_end = ?",
                SchemaConstants.SYSTEM_KEYSPACE_NAME,
                SystemKeyspace.SIZE_ESTIMATES);

        ResultSet resultSet = session.execute(session.prepare(query).bind(keyspace, table, tokenRange.getStart().toString(), tokenRange.getEnd().toString()));

        Row row = resultSet.one();

        long meanPartitionSize = 0;
        long partitionCount = 0;
        int splitCount = 0;

        if (row != null) {
            meanPartitionSize = row.getLong("mean_partition_size");
            partitionCount = row.getLong("partitions_count");

            splitCount = splitSizeMb > 0
                    ? (int) (meanPartitionSize * partitionCount / splitSizeMb / 1024 / 1024)
                    : (int) (partitionCount / splitSize);
        }

        // If we have no data on this split or the size estimate is 0,
        // return the full split i.e., do not sub-split
        // Assume smallest granularity of partition count available from CASSANDRA-7688
        if (splitCount == 0) {
            Map<TokenRange, Long> wrappedTokenRange = new HashMap<>();
            wrappedTokenRange.put(tokenRange, (long) 128);
            return wrappedTokenRange;
        }

        List<TokenRange> splitRanges = tokenRange.splitEvenly(splitCount);
        Map<TokenRange, Long> rangesWithLength = new HashMap<>();
        for (TokenRange range : splitRanges) {
            rangesWithLength.put(range, partitionCount / splitCount);
        }
        return rangesWithLength;
    }

    // Old Hadoop API
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        TaskAttemptContext tac = HadoopCompat.newTaskAttemptContext(jobConf, new TaskAttemptID());
        List<org.apache.hadoop.mapreduce.InputSplit> newInputSplits = this.getSplits(tac);
        InputSplit[] oldInputSplits = new InputSplit[newInputSplits.size()];
        for (int i = 0; i < newInputSplits.size(); i++) {
            oldInputSplits[i] = (ColumnFamilySplit) newInputSplits.get(i);
        }
        return oldInputSplits;
    }

    public static CqlSession getInputSession(String[] hosts, Configuration conf) {
        int port = getInputNativePort(conf);
        return getSession(hosts, conf, port);
    }

    private static int getInputNativePort(Configuration conf) {
        return Integer.parseInt(conf.get(STORAGE_PORT.name(), "9042"));
    }


    // BIG TODO: add support for SSL stuff and friends
    private static CqlSession getSession(String[] hosts, Configuration conf, int port) {
        return CqlSession.builder()
                .addContactPoints(
                        Arrays.stream(hosts)
                                .map(s -> new InetSocketAddress(s, port))
                                .collect(Collectors.toList())
                )
                .withLocalDatacenter("datacenter1")
                .build();
    }

    /**
     * Gets a token tokenRange and splits it up according to the suggested
     * size into input splits that Hadoop can use.
     */
    class SplitCallable implements Callable<List<org.apache.hadoop.mapreduce.InputSplit>> {

        private final TokenRange tokenRange;
        private final Set<Node> hosts;
        private final Configuration conf;
        private final CqlSession session;

        public SplitCallable(TokenRange tr, Set<Node> hosts, Configuration conf, CqlSession session) {
            this.tokenRange = tr;
            this.hosts = hosts;
            this.conf = conf;
            this.session = session;
        }

        public List<org.apache.hadoop.mapreduce.InputSplit> call() throws Exception {
            ArrayList<org.apache.hadoop.mapreduce.InputSplit> splits = new ArrayList<>();
            Map<TokenRange, Long> subSplits;
            subSplits = getSubSplits(keyspace, cfName, tokenRange, conf, session);
            // turn the sub-ranges into InputSplits
            String[] endpoints = new String[hosts.size()];

            // hadoop needs hostname, not ip
            int endpointIndex = 0;
            for (Node endpoint : hosts) {
                endpoints[endpointIndex++] = endpoint.getListenAddress().get().getHostName();
            }

            boolean partitionerIsOpp = partitioner instanceof OrderPreservingPartitioner || partitioner instanceof ByteOrderedPartitioner;

            for (Map.Entry<TokenRange, Long> subSplitEntry : subSplits.entrySet()) {
                List<TokenRange> ranges = subSplitEntry.getKey().unwrap();
                for (TokenRange subrange : ranges) {
                    ColumnFamilySplit split;
                    if (subrange instanceof Murmur3TokenRange) {
                        Murmur3Token startToken = (Murmur3Token) subrange.getStart();
                        Murmur3Token endToken = (Murmur3Token) subrange.getEnd();
                        split = new ColumnFamilySplit(
                                Long.toString(startToken.getValue()),
                                Long.toString(endToken.getValue()),
                                subSplitEntry.getValue(),
                                endpoints);
                    } else {
                        split = new ColumnFamilySplit(
                                partitionerIsOpp ? subrange.getStart().toString().substring(2) : subrange.getStart().toString(),
                                partitionerIsOpp ? subrange.getEnd().toString().substring(2) : subrange.getEnd().toString(),
                                subSplitEntry.getValue(),
                                endpoints);
                    }


                    LOG.trace("adding {}", split);
                    splits.add(split);
                }
            }
            return splits;
        }
    }
}
