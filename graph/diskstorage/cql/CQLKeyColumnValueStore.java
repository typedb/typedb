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

package grakn.core.graph.diskstorage.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.CompactionStrategy;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.EntryMetaData;
import grakn.core.graph.diskstorage.PermanentBackendException;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.TemporaryBackendException;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.keycolumnvalue.KCVMutation;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyIterator;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyRangeQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.KeySliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import grakn.core.graph.diskstorage.util.StaticArrayBuffer;
import grakn.core.graph.diskstorage.util.StaticArrayEntry;
import grakn.core.graph.diskstorage.util.StaticArrayEntryList;
import io.vavr.Lazy;
import io.vavr.Tuple;
import io.vavr.Tuple3;
import io.vavr.collection.Array;
import io.vavr.collection.Iterator;
import io.vavr.control.Try;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.leveledCompactionStrategy;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.timeWindowCompactionStrategy;
import static com.datastax.oss.driver.api.querybuilder.select.Selector.column;
import static grakn.core.graph.diskstorage.Backend.EDGESTORE_NAME;
import static grakn.core.graph.diskstorage.Backend.INDEXSTORE_NAME;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION_BLOCK_SIZE;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.CF_COMPRESSION_TYPE;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.COMPACTION_OPTIONS;
import static grakn.core.graph.diskstorage.cql.CQLConfigOptions.COMPACTION_STRATEGY;
import static grakn.core.graph.diskstorage.cql.CQLTransaction.getTransaction;
import static grakn.core.graph.graphdb.configuration.GraphDatabaseConfiguration.PAGE_SIZE;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.Predicates.instanceOf;

/**
 * An implementation of KeyColumnValueStore which stores the data in a CQL connected backend.
 */
public class CQLKeyColumnValueStore implements KeyColumnValueStore {

    private static final String TTL_FUNCTION_NAME = "ttl";
    private static final String WRITETIME_FUNCTION_NAME = "writetime";

    public static final String KEY_COLUMN_NAME = "key";
    public static final String COLUMN_COLUMN_NAME = "column1";
    public static final String VALUE_COLUMN_NAME = "value";
    static final String WRITETIME_COLUMN_NAME = "writetime";
    static final String TTL_COLUMN_NAME = "ttl";

    private static final String KEY_BINDING = "key";
    private static final String COLUMN_BINDING = "column1";
    private static final String VALUE_BINDING = "value";
    private static final String TIMESTAMP_BINDING = "timestamp";
    private static final String TTL_BINDING = "ttl";
    private static final String SLICE_START_BINDING = "sliceStart";
    private static final String SLICE_END_BINDING = "sliceEnd";
    private static final String KEY_START_BINDING = "keyStart";
    private static final String KEY_END_BINDING = "keyEnd";
    private static final String LIMIT_BINDING = "maxRows";

    static final Function<? super Throwable, BackendException> EXCEPTION_MAPPER = cause -> Match(cause).of(
            Case($(instanceOf(QueryValidationException.class)), PermanentBackendException::new),
            Case($(), () -> new TemporaryBackendException(cause.getMessage(), false)));

    private final CQLStoreManager storeManager;
    private final CqlSession session;
    private final String tableName;
    private final CQLColValGetter getter;
    private final Runnable closer;

    private final PreparedStatement getSlice;
    private final PreparedStatement getKeysAll;
    private final PreparedStatement getKeysRanged;
    private final PreparedStatement deleteColumn;
    private final PreparedStatement insertColumn;
    private final PreparedStatement insertColumnWithTTL;
    private final int pageSize;

    /**
     * Creates an instance of the KeyColumnValueStore that stores the data in a CQL backed table.
     *
     * @param storeManager  the CQLStoreManager that maintains the list of CQLKeyColumnValueStores
     * @param tableName     the name of the database table for storing the key/column/values
     * @param configuration data used in creating this store
     * @param closer        callback used to clean up references to this store in the store manager
     */
    CQLKeyColumnValueStore(CQLStoreManager storeManager, String tableName, Configuration configuration, Runnable closer) {

        this.storeManager = storeManager;
        this.tableName = tableName;
        this.closer = closer;
        this.session = this.storeManager.getSession();
        // NOTE: storeManager now only has access to localConfig (check JanusGraphFactory),
        // it gets initialised before reading globalConfig, so getMetaDataSchema will probably fail as it need to read configs from `system_properties`)
        // This is a temporary tradeoff so that we dont have to init StoreManager twice!!
        this.getter = new CQLColValGetter(storeManager.getMetaDataSchema(this.tableName)); // NOTE: this is reading only local config (not reading global configs from system_properties as originally designed)

        // Default configured page size for this storage backend. The page size is used to determine
        // the number of records to request at a time when streaming result data.
        this.pageSize = configuration.get(PAGE_SIZE);


        if (shouldInitialiseTable()) {
            initialiseTable(this.storeManager.getKeyspaceName(), tableName, configuration);
        }

        this.getSlice = this.session.prepare(selectFrom(this.storeManager.getKeyspaceName(), this.tableName)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .function(WRITETIME_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME)
                .function(TTL_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME)
                .where(
                        Relation.column(KEY_COLUMN_NAME).isEqualTo(bindMarker(KEY_BINDING)),
                        Relation.column(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING)),
                        Relation.column(COLUMN_COLUMN_NAME).isLessThan(bindMarker(SLICE_END_BINDING))
                )
                .limit(bindMarker(LIMIT_BINDING)).build());

        this.getKeysRanged = this.session.prepare(selectFrom(this.storeManager.getKeyspaceName(), this.tableName)
                .column(KEY_COLUMN_NAME)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .function(WRITETIME_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME)
                .function(TTL_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME)
                .allowFiltering()
                .where(
                        Relation.token(KEY_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(KEY_START_BINDING)),
                        Relation.token(KEY_COLUMN_NAME).isLessThan(bindMarker(KEY_END_BINDING))
                )
                .whereColumn(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING))
                .whereColumn(COLUMN_COLUMN_NAME).isLessThanOrEqualTo(bindMarker(SLICE_END_BINDING))
                .build());

        this.getKeysAll = this.session.prepare(selectFrom(this.storeManager.getKeyspaceName(), this.tableName)
                .column(KEY_COLUMN_NAME)
                .column(COLUMN_COLUMN_NAME)
                .column(VALUE_COLUMN_NAME)
                .function(WRITETIME_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(WRITETIME_COLUMN_NAME)
                .function(TTL_FUNCTION_NAME, column(VALUE_COLUMN_NAME)).as(TTL_COLUMN_NAME)
                .allowFiltering()
                .whereColumn(COLUMN_COLUMN_NAME).isGreaterThanOrEqualTo(bindMarker(SLICE_START_BINDING))
                .whereColumn(COLUMN_COLUMN_NAME).isLessThan(bindMarker(SLICE_END_BINDING))
                .build());

        this.deleteColumn = this.session.prepare(deleteFrom(this.storeManager.getKeyspaceName(), this.tableName)
                .usingTimestamp(bindMarker(TIMESTAMP_BINDING))
                .whereColumn(KEY_COLUMN_NAME).isEqualTo(bindMarker(KEY_BINDING))
                .whereColumn(COLUMN_COLUMN_NAME).isEqualTo(bindMarker(COLUMN_BINDING))
                .build());

        this.insertColumn = this.session.prepare(insertInto(this.storeManager.getKeyspaceName(), this.tableName)
                .value(KEY_COLUMN_NAME, bindMarker(KEY_BINDING))
                .value(COLUMN_COLUMN_NAME, bindMarker(COLUMN_BINDING))
                .value(VALUE_COLUMN_NAME, bindMarker(VALUE_BINDING))
                .usingTimestamp(bindMarker(TIMESTAMP_BINDING)).build());

        this.insertColumnWithTTL = this.session.prepare(insertInto(this.storeManager.getKeyspaceName(), this.tableName)
                .value(KEY_COLUMN_NAME, bindMarker(KEY_BINDING))
                .value(COLUMN_COLUMN_NAME, bindMarker(COLUMN_BINDING))
                .value(VALUE_COLUMN_NAME, bindMarker(VALUE_BINDING))
                .usingTimestamp(bindMarker(TIMESTAMP_BINDING))
                .usingTtl(bindMarker(TTL_BINDING)).build());
    }

    /**
     * Check if the current table should be initialised.
     * NOTE: This additional check is needed when Cassandra security is enabled, for more info check issue #1103
     *
     * @return true if table already exists in current keyspace, false otherwise
     */
    private boolean shouldInitialiseTable() {
        return this.session.getMetadata()
                .getKeyspace(storeManager.getKeyspaceName()).map(k -> !k.getTable(this.tableName).isPresent())
                .orElse(true);
    }

    private void initialiseTable(String keyspaceName, String tableName, Configuration configuration) {
        CreateTableWithOptions createTable = createTable(keyspaceName, tableName)
                .ifNotExists()
                .withPartitionKey(KEY_COLUMN_NAME, DataTypes.BLOB)
                .withClusteringColumn(COLUMN_COLUMN_NAME, DataTypes.BLOB)
                .withColumn(VALUE_COLUMN_NAME, DataTypes.BLOB)
                .withSpeculativeRetry("NONE");


        // The following caching settings are copied from old Janus - need to verify if they actually provide any performance gain
        if (tableName.startsWith(EDGESTORE_NAME)) {
            createTable = createTable.withCaching(true, SchemaBuilder.RowsPerPartition.NONE);
        }
        if (tableName.startsWith(INDEXSTORE_NAME)) {
            createTable = createTable.withCaching(false, SchemaBuilder.RowsPerPartition.rows(100));
        }

        createTable = compactionOptions(createTable, configuration);

        createTable = compressionOptions(createTable, configuration);

        this.storeManager.executeOnSession(createTable.build());
    }

    private static CreateTableWithOptions compressionOptions(CreateTableWithOptions createTable, Configuration configuration) {
        if (!configuration.get(CF_COMPRESSION)) {
            // No compression
            return createTable.withNoCompression();
        }
        String compressionType = configuration.get(CF_COMPRESSION_TYPE);
        int chunkLengthInKb = configuration.get(CF_COMPRESSION_BLOCK_SIZE);

        return createTable.withOption("compression",
                ImmutableMap.of("class", compressionType, "chunk_length_kb", chunkLengthInKb));

    }

    private static CreateTableWithOptions compactionOptions(CreateTableWithOptions createTable, Configuration configuration) {
        if (!configuration.has(COMPACTION_STRATEGY)) {
            return createTable;
        }

        CompactionStrategy<?> compactionStrategy = Match(configuration.get(COMPACTION_STRATEGY))
                .of(
                        Case($("SizeTieredCompactionStrategy"), leveledCompactionStrategy()),
                        Case($("TimeWindowCompactionStrategy"), timeWindowCompactionStrategy()),
                        Case($("LeveledCompactionStrategy"), leveledCompactionStrategy()));
        Iterator<Array<String>> groupedOptions = Array.of(configuration.get(COMPACTION_OPTIONS))
                .grouped(2);

        for (Array<String> keyValue : groupedOptions) {
            compactionStrategy = compactionStrategy.withOption(keyValue.get(0), keyValue.get(1));
        }

        return createTable.withCompaction(compactionStrategy);
    }

    @Override
    public void close() {
        this.closer.run();
    }

    @Override
    public String getName() {
        return this.tableName;
    }

    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) {
        ResultSet result = this.storeManager.executeOnSession(this.getSlice.bind()
                .setByteBuffer(KEY_BINDING, query.getKey().asByteBuffer())
                .setByteBuffer(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                .setByteBuffer(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                .setInt(LIMIT_BINDING, query.getLimit())
                .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()));

        return fromResultSet(result, this.getter);
    }

    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("The CQL backend does not support multi-key queries");
    }

    private static EntryList fromResultSet(ResultSet resultSet, StaticArrayEntry.GetColVal<Tuple3<StaticBuffer, StaticBuffer, Row>, StaticBuffer> getter) {
        Lazy<ArrayList<Row>> lazyList = Lazy.of(() -> Lists.newArrayList(resultSet));

        // Use the Iterable overload of of ByteBuffer as it's able to allocate
        // the byte array up front.
        // To ensure that the Iterator instance is recreated, it is created
        // within the closure otherwise
        // the same iterator would be reused and would be exhausted.
        return StaticArrayEntryList.ofStaticBuffer(() -> Iterator.ofAll(lazyList.get()).map(row -> Tuple.of(
                StaticArrayBuffer.of(row.getByteBuffer(COLUMN_COLUMN_NAME)),
                StaticArrayBuffer.of(row.getByteBuffer(VALUE_COLUMN_NAME)),
                row)),
                getter);
    }

    /*
     * Used by CQLStoreManager
     */
    BatchableStatement<BoundStatement> deleteColumn(StaticBuffer key, StaticBuffer column, long timestamp) {
        return this.deleteColumn.bind()
                .setByteBuffer(KEY_BINDING, key.asByteBuffer())
                .setByteBuffer(COLUMN_BINDING, column.asByteBuffer())
                .setLong(TIMESTAMP_BINDING, timestamp);
    }

    /*
     * Used by CQLStoreManager
     */
    BatchableStatement<BoundStatement> insertColumn(StaticBuffer key, Entry entry, long timestamp) {
        Integer ttl = (Integer) entry.getMetaData().get(EntryMetaData.TTL);
        if (ttl != null) {
            return this.insertColumnWithTTL.bind()
                    .setByteBuffer(KEY_BINDING, key.asByteBuffer())
                    .setByteBuffer(COLUMN_BINDING, entry.getColumn().asByteBuffer())
                    .setByteBuffer(VALUE_BINDING, entry.getValue().asByteBuffer())
                    .setLong(TIMESTAMP_BINDING, timestamp)
                    .setInt(TTL_BINDING, ttl);
        }
        return this.insertColumn.bind()
                .setByteBuffer(KEY_BINDING, key.asByteBuffer())
                .setByteBuffer(COLUMN_BINDING, entry.getColumn().asByteBuffer())
                .setByteBuffer(VALUE_BINDING, entry.getValue().asByteBuffer())
                .setLong(TIMESTAMP_BINDING, timestamp);
    }

    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        this.storeManager.mutateMany(Collections.singletonMap(this.tableName, Collections.singletonMap(key, new KCVMutation(additions, deletions))), txh);
    }

    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        if (!this.storeManager.getFeatures().hasOrderedScan()) {
            throw new PermanentBackendException("This operation is only allowed when the byteorderedpartitioner is used.");
        }

        TokenMap tokenMap = this.session.getMetadata().getTokenMap().get();
        return Try.of(() -> new CQLResultSetKeyIterator(
                query,
                this.getter,
                this.storeManager.executeOnSession(this.getKeysRanged.bind()
                        .setToken(KEY_START_BINDING, tokenMap.newToken(query.getKeyStart().asByteBuffer()))
                        .setToken(KEY_END_BINDING, tokenMap.newToken(query.getKeyEnd().asByteBuffer()))
                        .setByteBuffer(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                        .setByteBuffer(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                        .setPageSize(this.pageSize)
                        .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()))))
                .getOrElseThrow(EXCEPTION_MAPPER);
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        if (this.storeManager.getFeatures().hasOrderedScan()) {
            throw new PermanentBackendException("This operation is only allowed when a random partitioner (md5 or murmur3) is used.");
        }

        return Try.of(() -> new CQLResultSetKeyIterator(
                query,
                this.getter,
                this.storeManager.executeOnSession(this.getKeysAll.bind()
                        .setByteBuffer(SLICE_START_BINDING, query.getSliceStart().asByteBuffer())
                        .setByteBuffer(SLICE_END_BINDING, query.getSliceEnd().asByteBuffer())
                        .setPageSize(this.pageSize)
                        .setConsistencyLevel(getTransaction(txh).getReadConsistencyLevel()))))
                .getOrElseThrow(EXCEPTION_MAPPER);
    }
}
