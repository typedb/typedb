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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3TokenRange;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
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
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.StringUtils;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
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

public class InputFormatGrakn extends org.apache.hadoop.mapreduce.InputFormat<Long, Row> implements org.apache.hadoop.mapred.InputFormat<Long, Row> {
    private static final String MAPRED_TASK_ID = "mapred.task.id";
    private static final Logger LOG = LoggerFactory.getLogger(InputFormatGrakn.class);

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


        RecordReaderGrakn recordReader = new RecordReaderGrakn();
        recordReader.initialize((org.apache.hadoop.mapreduce.InputSplit) split, tac);
        return recordReader;
    }

    @Override
    public org.apache.hadoop.mapreduce.RecordReader<Long, Row> createRecordReader(org.apache.hadoop.mapreduce.InputSplit arg0, TaskAttemptContext arg1) {
        return new RecordReaderGrakn();
    }

    private void validateConfiguration(Configuration conf) {
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

    static CqlSession getInputSession(String[] hosts, Configuration conf) {
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
    private class SplitCallable implements Callable<List<org.apache.hadoop.mapreduce.InputSplit>> {

        private final TokenRange tokenRange;
        private final Set<Node> hosts;
        private final Configuration conf;
        private final CqlSession session;

        SplitCallable(TokenRange tr, Set<Node> hosts, Configuration conf, CqlSession session) {
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

    /**
     * CqlRecordReader reads the rows return from the CQL query
     * It uses CQL auto-paging.
     *
     * Return a Long as a local CQL row key starts from 0;
     *
     * {@code
     * Row as C* java driver CQL result set row
     * 1) select clause must include partition key columns (to calculate the progress based on the actual CF row processed)
     * 2) where clause must include token(partition_key1, ...  , partition_keyn) > ? and
     * token(partition_key1, ... , partition_keyn) <= ?  (in the right order)
     * }
     */
    static class RecordReaderGrakn extends org.apache.hadoop.mapreduce.RecordReader<Long, Row> implements RecordReader<Long, Row>, AutoCloseable {
        private static final Logger LOG = LoggerFactory.getLogger(RecordReaderGrakn.class);
        private static final String INPUT_CQL_COLUMNS_CONFIG = "cassandra.input.columnfamily.columns";
        private static final String INPUT_CQL_WHERE_CLAUSE_CONFIG = "cassandra.input.where.clause";
        private static final String INPUT_CQL = "cassandra.input.cql";

        private ColumnFamilySplit split;
        private RowIterator rowIterator;

        private Pair<Long, Row> currentRow;
        private int totalRowCount; // total number of rows to fetch
        private String keyspace;
        private String cfName;
        private String cqlQuery;
        private CqlSession session;
        private IPartitioner partitioner;
        private String inputColumns;
        private String userDefinedWhereClauses;

        private List<String> partitionKeys = new ArrayList<>();

        // partition keys -- key aliases
        private LinkedHashMap<String, Boolean> partitionBoundColumns = Maps.newLinkedHashMap();
        private int nativeProtocolVersion = 1;

        RecordReaderGrakn() {
            super();
        }

        @Override
        public void initialize(org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context) throws IOException {
            this.split = (ColumnFamilySplit) split;
            Configuration conf = HadoopCompat.getConfiguration(context);
            totalRowCount = (this.split.getLength() < Long.MAX_VALUE)
                    ? (int) this.split.getLength() : ConfigHelper.getInputSplitSize(conf);
            cfName = ConfigHelper.getInputColumnFamily(conf);
            keyspace = ConfigHelper.getInputKeyspace(conf);
            partitioner = ConfigHelper.getInputPartitioner(conf);
            inputColumns = conf.get(INPUT_CQL_COLUMNS_CONFIG);
            userDefinedWhereClauses = conf.get(INPUT_CQL_WHERE_CLAUSE_CONFIG);

            try {

                // create a Cluster instance
                String[] locations = split.getLocations();
                session = getInputSession(locations, conf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            //get negotiated serialization protocol
            nativeProtocolVersion = session.getContext().getProtocolVersion().getCode();

            // If the user provides a CQL query then we will use it without validation
            // otherwise we will fall back to building a query using the:
            //   inputColumns
            //   whereClauses
            cqlQuery = conf.get(INPUT_CQL);
            // validate that the user hasn't tried to give us a custom query along with input columns
            // and where clauses
            if (StringUtils.isNotEmpty(cqlQuery) && (StringUtils.isNotEmpty(inputColumns) || StringUtils.isNotEmpty(userDefinedWhereClauses))) {
                throw new AssertionError("Cannot define a custom query with input columns and / or where clauses");
            }

            if (StringUtils.isEmpty(cqlQuery)) {
                cqlQuery = buildQuery();
            }
            LOG.trace("cqlQuery {}", cqlQuery);

            rowIterator = new RowIterator();
            LOG.trace("created {}", rowIterator);
        }

        public void close() {
            if (session != null) {
                session.close();
            }
        }

        public Long getCurrentKey() {
            return currentRow.left;
        }

        public Row getCurrentValue() {
            return currentRow.right;
        }

        public float getProgress() {
            if (!rowIterator.hasNext()) {
                return 1.0F;
            }

            // the progress is likely to be reported slightly off the actual but close enough
            float progress = ((float) rowIterator.totalRead / totalRowCount);
            return progress > 1.0F ? 1.0F : progress;
        }

        public boolean nextKeyValue() throws IOException {
            if (!rowIterator.hasNext()) {
                LOG.trace("Finished scanning {} rows (estimate was: {})", rowIterator.totalRead, totalRowCount);
                return false;
            }

            try {
                currentRow = rowIterator.next();
            } catch (Exception e) {
                // throw it as IOException, so client can catch it and handle it at client side
                IOException ioe = new IOException(e.getMessage());
                ioe.initCause(ioe.getCause());
                throw ioe;
            }
            return true;
        }

        // Because the old Hadoop API wants us to write to the key and value
        // and the new asks for them, we need to copy the output of the new API
        // to the old. Thus, expect a small performance hit.
        // And obviously this wouldn't work for wide rows. But since ColumnFamilyInputFormat
        // and ColumnFamilyRecordReader don't support them, it should be fine for now.
        public boolean next(Long key, Row value) throws IOException {
            if (nextKeyValue()) {
                ((WrappedRow) value).setRow(getCurrentValue());
                return true;
            }
            return false;
        }

        public long getPos() {
            return rowIterator.totalRead;
        }

        public Long createKey() {
            return Long.valueOf(0L);
        }

        public Row createValue() {
            return new WrappedRow();
        }

        /**
         * Return native version protocol of the cluster connection
         *
         * @return serialization protocol version.
         */
        public int getNativeProtocolVersion() {
            return nativeProtocolVersion;
        }

        /**
         * CQL row iterator
         * Input cql query
         * 1) select clause must include key columns (if we use partition key based row count)
         * 2) where clause must include token(partition_key1 ... partition_keyn) > ? and
         * token(partition_key1 ... partition_keyn) <= ?
         */
        private class RowIterator extends AbstractIterator<Pair<Long, Row>> {
            private long keyId = 0L;
            private Map<String, ByteBuffer> previousRowKey = new HashMap<>(); // previous CF row key
            int totalRead = 0; // total number of cf rows read
            Iterator<Row> rows;

            public RowIterator() {
                AbstractType type = partitioner.getTokenValidator();
                ResultSet rs = session.execute(session.prepare(cqlQuery).bind(type.compose(type.fromString(split.getStartToken())), type.compose(type.fromString(split.getEndToken()))));
                for (ColumnMetadata meta : session.getMetadata().getKeyspace(quote(keyspace)).get().getTable(quote(cfName)).get().getPartitionKey()) {
                    partitionBoundColumns.put(meta.getName().toString(), Boolean.TRUE);
                }
                rows = rs.iterator();
            }

            protected Pair<Long, Row> computeNext() {
                if (rows == null || !rows.hasNext()) {
                    return endOfData();
                }

                Row row = rows.next();
                Map<String, ByteBuffer> keyColumns = new HashMap<String, ByteBuffer>(partitionBoundColumns.size());
                for (String column : partitionBoundColumns.keySet()) {
                    keyColumns.put(column, row.getBytesUnsafe(column));
                }

                // increase total CF row read
                if (previousRowKey.isEmpty() && !keyColumns.isEmpty()) {
                    previousRowKey = keyColumns;
                    totalRead++;
                } else {
                    for (String column : partitionBoundColumns.keySet()) {
                        // this is not correct - but we don't seem to have easy access to better type information here
                        if (ByteBufferUtil.compareUnsigned(keyColumns.get(column), previousRowKey.get(column)) != 0) {
                            previousRowKey = keyColumns;
                            totalRead++;
                            break;
                        }
                    }
                }
                keyId++;
                return Pair.create(keyId, row);
            }
        }

        private static class WrappedRow implements Row {
            private Row row;

            public void setRow(Row row) {
                this.row = row;
            }

            @Override
            public ColumnDefinitions getColumnDefinitions() {
                return row.getColumnDefinitions();
            }

            @Override
            public int firstIndexOf(CqlIdentifier id) {
                return row.firstIndexOf(id);
            }

            @Override
            public DataType getType(CqlIdentifier id) {
                return row.getType(id);
            }

            @Override
            public int firstIndexOf(String name) {
                return row.firstIndexOf(name);
            }

            @Override
            public DataType getType(String name) {
                return row.getType(name);
            }

            @Override
            public ByteBuffer getBytesUnsafe(int i) {
                return row.getBytesUnsafe(i);
            }

            @Override
            public int size() {
                return row.size();
            }

            @Override
            public DataType getType(int i) {
                return row.getType(i);
            }

            @Override
            public CodecRegistry codecRegistry() {
                return row.codecRegistry();
            }

            @Override
            public ProtocolVersion protocolVersion() {
                return row.protocolVersion();
            }

            @Override
            public boolean isDetached() {
                return row.isDetached();
            }

            @Override
            public void attach(AttachmentPoint attachmentPoint) {
                row.attach(attachmentPoint);
            }
        }

        /**
         * Build a query for the reader of the form:
         * <p>
         * SELECT * FROM ks>cf token(pk1,...pkn)>? AND token(pk1,...pkn)<=? [AND user where clauses] [ALLOW FILTERING]
         */
        private String buildQuery() {
            fetchKeys();

            List<String> columns = getSelectColumns();
            String selectColumnList = columns.size() == 0 ? "*" : makeColumnList(columns);
            String partitionKeyList = makeColumnList(partitionKeys);

            return String.format("SELECT %s FROM %s.%s WHERE token(%s)>? AND token(%s)<=?" + getAdditionalWhereClauses(),
                    selectColumnList, quote(keyspace), quote(cfName), partitionKeyList, partitionKeyList);
        }

        private String getAdditionalWhereClauses() {
            String whereClause = "";
            if (StringUtils.isNotEmpty(userDefinedWhereClauses)) {
                whereClause += " AND " + userDefinedWhereClauses;
            }
            if (StringUtils.isNotEmpty(userDefinedWhereClauses)) {
                whereClause += " ALLOW FILTERING";
            }
            return whereClause;
        }

        private List<String> getSelectColumns() {
            List<String> selectColumns = new ArrayList<>();

            if (StringUtils.isNotEmpty(inputColumns)) {
                // We must select all the partition keys plus any other columns the user wants
                selectColumns.addAll(partitionKeys);
                for (String column : Splitter.on(',').split(inputColumns)) {
                    if (!partitionKeys.contains(column)) {
                        selectColumns.add(column);
                    }
                }
            }
            return selectColumns;
        }

        private String makeColumnList(Collection<String> columns) {
            return Joiner.on(',').join(Iterables.transform(columns, new Function<String, String>() {
                public String apply(String column) {
                    return quote(column);
                }
            }));
        }

        private void fetchKeys() {
            // get CF meta data
            TableMetadata tableMetadata = session
                    .getMetadata()
                    .getKeyspace(keyspace).get()
                    .getTable(cfName)
                    .orElseThrow(() -> new RuntimeException("No table metadata found for " + keyspace + "." + cfName));

            //Here we assume that tableMetadata.getPartitionKey() always
            //returns the list of columns in order of component_index
            for (ColumnMetadata partitionKey : tableMetadata.getPartitionKey()) {
                partitionKeys.add(partitionKey.getName().toString());
            }
        }

        private String quote(String identifier) {
            return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
        }
    }
}
