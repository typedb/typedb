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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * CqlRecordReader reads the rows return from the CQL query
 * It uses CQL auto-paging.
 * </p>
 * <p>
 * Return a Long as a local CQL row key starts from 0;
 * </p>
 * {@code
 * Row as C* java driver CQL result set row
 * 1) select clause must include partition key columns (to calculate the progress based on the actual CF row processed)
 * 2) where clause must include token(partition_key1, ...  , partition_keyn) > ? and
 * token(partition_key1, ... , partition_keyn) <= ?  (in the right order)
 * }
 */
public class GraknCqlRecordReader extends RecordReader<Long, Row> implements org.apache.hadoop.mapred.RecordReader<Long, Row>, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(GraknCqlRecordReader.class);

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
    protected int nativeProtocolVersion = 1;

    public GraknCqlRecordReader() {
        super();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        this.split = (ColumnFamilySplit) split;
        Configuration conf = HadoopCompat.getConfiguration(context);
        totalRowCount = (this.split.getLength() < Long.MAX_VALUE)
                ? (int) this.split.getLength() : ConfigHelper.getInputSplitSize(conf);
        cfName = ConfigHelper.getInputColumnFamily(conf);
        keyspace = ConfigHelper.getInputKeyspace(conf);
        partitioner = ConfigHelper.getInputPartitioner(conf);
        inputColumns = GraknCqlConfigHelper.getInputcolumns(conf);
        userDefinedWhereClauses = GraknCqlConfigHelper.getInputWhereClauses(conf);

        try {

            // create a Cluster instance
            String[] locations = split.getLocations();
            session = GraknInputFormat.getInputSession(locations, conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //get negotiated serialization protocol
        nativeProtocolVersion = session.getContext().getProtocolVersion().getCode();

        // If the user provides a CQL query then we will use it without validation
        // otherwise we will fall back to building a query using the:
        //   inputColumns
        //   whereClauses
        cqlQuery = GraknCqlConfigHelper.getInputCql(conf);
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

    public long getPos() throws IOException {
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
        protected int totalRead = 0; // total number of cf rows read
        protected Iterator<Row> rows;
        private Map<String, ByteBuffer> previousRowKey = new HashMap<>(); // previous CF row key

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
