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

package com.vaticle.typedb.core.rocks;

import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TableProperties;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.STORAGE_PROPERTY_EXCEPTION;

/**
 * A list of interesting properties to retrieve from RocksDB.
 * All properties are visible here:
 * https://github.com/facebook/rocksdb/blob/20357988345b02efcef303bc274089111507e160/include/rocksdb/db.h#L750
 */
public enum RocksProperties {

    ACTIVE_MEM_TABLE_SIZE("rocksdb.cur-size-active-mem-table", (size) ->
            String.format("%.2f mb", Double.parseDouble(size) / MB)),
    ALL_MEM_TABLES_SIZE("rocksdb.size-all-mem-tables", (size) ->
            String.format("%.2f mb", Double.parseDouble(size) / MB)),
    BLOCK_CACHE_PINNED_USAGE("rocksdb.block-cache-pinned-usage", (size) ->
            String.format("%.2f mb", Double.parseDouble(size) / MB)),
    BLOCK_CACHE_CAPACITY("rocksdb.block-cache-capacity", (size) ->
            String.format("%.2f mb", Double.parseDouble(size) / MB)),
    BLOCK_CACHE_USAGE("rocksdb.block-cache-usage", (size) ->
            String.format("%.2f mb", Double.parseDouble(size) / MB)),
    SST_READERS_MEMORY_ESTIMATE("rocksdb.estimate-table-readers-mem", (size) ->
            String.format("%.2f mb", Double.parseDouble(size) / MB)),
    ACTIVE_SNAPSHOTS("rocksdb.num-snapshots", (num) -> num),
    OLDEST_ACTIVE_SNAPSHOT_UNIX_SECONDS("rocksdb.oldest-snapshot-time", (oldestTime) ->
            String.format("%.2f sec", (System.currentTimeMillis() / 1000.0 - Long.parseLong(oldestTime)))),
    LIVE_VERSIONS("rocksdb.num-live-versions", (num) -> num),
    SST_FILES_LIVE_SIZE("rocksdb.live-sst-files-size", (size) ->
            String.format("%.2f mb", Double.parseDouble(size) / MB)),
    SST_FILES_HELD_SIZE("rocksdb.total-sst-files-size", (size) ->
            String.format("%.2f mb", Double.parseDouble(size) / MB)),
    MEM_TABLE_FLUSH_PENDING("rocksdb.mem-table-flush-pending", (size) ->
            String.format("%.2f mb", Double.parseDouble(size) / MB)),
    COMPACTION_BYTES_ESTIMATE("rocksdb.estimate-pending-compaction-bytes", (size) ->
            String.format("%.2f mb", Double.parseDouble(size) / MB)),
    NUM_RUNNING_COMPACTIONS("rocksdb.num-running-compactions", (num) -> num),
    NUM_RUNNING_FLUSH("rocksdb.num-running-flushes", (num) -> num);


    private final String property;
    private final Function<String, String> formatter;

    RocksProperties(String property, Function<String, String> formatter) {
        this.property = property;
        this.formatter = formatter;
    }

    private String getFormatted(OptimisticTransactionDB rocksDB) throws RocksDBException {
        return formatter.apply(rocksDB.getProperty(property));
    }

    static class Logger implements Runnable {

        private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RocksProperties.class);

        private final OptimisticTransactionDB rocksDB;
        private final String database;

        Logger(OptimisticTransactionDB rocksDB, String database) {
            this.rocksDB = rocksDB;
            this.database = database;
        }

        @Override
        public void run() {
            logRocksProperties();
            logDataProperties();
        }

        private void logRocksProperties() {
            try {
                StringBuilder builder = new StringBuilder(String.format("Database '%s' rocksdb properties:\n", database));
                for (RocksProperties property : RocksProperties.values()) {
                    builder.append(String.format("%-40s %s\n", property.name().toLowerCase(), property.getFormatted(rocksDB)));
                }
                LOG.debug(builder.toString());
            } catch (RocksDBException e) {
                LOG.error(STORAGE_PROPERTY_EXCEPTION.message(), e);
            }
        }

        private void logDataProperties() {
            try {
                long dataBlocksSize = 0;
                long indexBlocksSize = 0;
                long filterBlocksSize = 0;
                long rawKeysSize = 0;
                long rawValuesSize = 0;
                long numEntries = 0;
                long numDeletions = 0;
                Map<String, TableProperties> sstProperties = rocksDB.getPropertiesOfAllTables();
                for (Map.Entry<String, TableProperties> entry : sstProperties.entrySet()) {
                    TableProperties properties = entry.getValue();
                    dataBlocksSize += properties.getDataSize();
                    indexBlocksSize += properties.getIndexSize();
                    filterBlocksSize += properties.getFilterSize();
                    rawKeysSize += properties.getRawKeySize();
                    rawValuesSize += properties.getRawValueSize();
                    numEntries += properties.getNumEntries();
                    numDeletions += properties.getNumDeletions();
                }

                long keysEstimate = numEntries - numDeletions;
                String formattedSummary = String.format("%-40s %.2f mb\n", "Data blocks size", ((float) dataBlocksSize / MB)) +
                        String.format("%-40s %.2f mb\n", "Index blocks size", ((float) indexBlocksSize / MB)) +
                        String.format("%-40s %.2f mb\n", "Filter blocks size", ((float) filterBlocksSize / MB)) +
                        String.format("%-40s %.2f mb\n", "Raw keys size", ((float) rawKeysSize / MB)) +
                        String.format("%-40s %.2f mb\n", "Raw values size", ((float) rawValuesSize / MB)) +
                        String.format("%-40s %d\n", "Approximate total keys", keysEstimate) +
                        String.format("%-40s %.1f bytes\n", "Bytes per key (raw values/approx keys)", (float) rawKeysSize / (keysEstimate)) +
                        String.format("%-40s %.1f bytes\n", "Bytes per value (raw values/approx keys)", (float) rawValuesSize / (keysEstimate));

                LOG.debug("Database '{}' summary from '{}' SST files:\n{}\n", database, sstProperties.size(), formattedSummary);
            } catch (RocksDBException e) {
                LOG.error(STORAGE_PROPERTY_EXCEPTION.message(), e);
            }
        }
    }
}
