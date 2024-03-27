/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.database;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TableProperties;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;
import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.STORAGE_PROPERTY_EXCEPTION;
import static java.lang.String.format;
import static java.util.Arrays.stream;

/**
 * A list of interesting properties to retrieve from RocksDB.
 * All properties are visible here:
 * https://github.com/facebook/rocksdb/blob/20357988345b02efcef303bc274089111507e160/include/rocksdb/db.h#L750
 */
public class RocksProperties {

    static final Map<String, Property> properties = map(
            pair("memtable-size-active-bytes" , new Property.Byte("Active memtable size", "rocksdb.cur-size-active-mem-table", true)),
            pair("memtable-size-total-bytes" , new Property.Byte("All memtables size", "rocksdb.size-all-mem-tables", true)),
            pair("memtable-flush-pending-bytes" , new Property.Byte("Pending memtable flush size", "rocksdb.mem-table-flush-pending", true)),
            pair("memtable-flush-running-count" , new Property.Number("Running flushes", "rocksdb.num-running-flushes", true)),
            pair("block-cache-bytes" , new Property.Byte("Block cache capacity", "rocksdb.block-cache-capacity", false)),
            pair("block-cache-usage-bytes" , new Property.Byte("Block cache usage", "rocksdb.block-cache-usage", false)),
            pair("block-cache-usage-pinned-bytes" , new Property.Byte("Block cache pinned usage", "rocksdb.block-cache-pinned-usage", false)),
            pair("pinned-sst-reader-memory-bytes" , new Property.Byte("Est. SST readers pinned memory", "rocksdb.estimate-table-readers-mem", true)),
            pair("snapshot-active-count" , new Property.Number("Active snapshots", "rocksdb.num-snapshots", false)),
            pair("snapshot-oldest-sec" , new Property.Seconds("Oldest active snapshot", "rocksdb.oldest-snapshot-time", false)),
            pair("lsm-tree-versions-live-count" , new Property.Number("Live LSM tree versions", "rocksdb.num-live-versions", true)),
            pair("compaction-pending-bytes" , new Property.Byte("Estimated pending compaction size", "rocksdb.estimate-pending-compaction-bytes", true)),
            pair("compaction-running-count" , new Property.Number("Running compactions", "rocksdb.num-running-compactions", true)),
            pair("key-count" , new Property.Number("Est. total keys", "rocksdb.estimate-num-keys", true)),
            pair("disk-size-total-bytes" , new Property.Byte("Est. total disk size", "rocksdb.total-sst-files-size", true)),
            pair("disk-size-live-bytes" , new Property.Byte("Live SST files size", "rocksdb.live-sst-files-size", true))
    );

    static abstract class Property {

        private final String label;
        private final String property;
        private final boolean applicablePerColumnFamily;

        private Property(String label, String property, boolean applicablePerColumnFamily) {
            this.label = label;
            this.property = property;
            this.applicablePerColumnFamily = applicablePerColumnFamily;
        }

        long get(OptimisticTransactionDB rocksDB) throws RocksDBException {
            assert !applicablePerColumnFamily;
            return rocksDB.getLongProperty(property);
        }

        long get(OptimisticTransactionDB rocksDB, ColumnFamilyHandle cf) throws RocksDBException {
            assert applicablePerColumnFamily;
            return rocksDB.getLongProperty(cf, property);
        }

        abstract String getFormatted(OptimisticTransactionDB rocksDB) throws RocksDBException;

        abstract String getFormatted(OptimisticTransactionDB rocksDB, List<ColumnFamilyHandle> cfHandles) throws RocksDBException;

        public String label() {
            return label;
        }

        boolean isApplicablePerColumnFamily() {
            return applicablePerColumnFamily;
        }

        abstract String unitName();

        private static class Byte extends Property {

            Byte(String label, String property, boolean applicablePerCF) {
                super(label, property, applicablePerCF);
            }

            @Override
            String getFormatted(OptimisticTransactionDB rocksDB) throws RocksDBException {
                assert !isApplicablePerColumnFamily();
                return format("%s mb", toMbString(get(rocksDB)));
            }

            @Override
            String getFormatted(OptimisticTransactionDB rocksDB, List<ColumnFamilyHandle> cfHandles) throws RocksDBException {
                assert isApplicablePerColumnFamily();
                long[] bytes = new long[cfHandles.size()];
                for (int i = 0; i < cfHandles.size(); i++) {
                    bytes[i] = get(rocksDB, cfHandles.get(i));
                }
                return formatBytesToMB(bytes);
            }

            @Override
            String unitName() {
                return "byte";
            }
        }

        private static class Number extends Property {

            Number(String label, String property, boolean applicablePerCF) {
                super(label, property, applicablePerCF);
            }

            @Override
            String getFormatted(OptimisticTransactionDB rocksDB) throws RocksDBException {
                assert !isApplicablePerColumnFamily();
                return "" + get(rocksDB);
            }

            @Override
            String getFormatted(OptimisticTransactionDB rocksDB, List<ColumnFamilyHandle> cfHandles) throws RocksDBException {
                assert isApplicablePerColumnFamily();
                List<String> formatted = new ArrayList<>();
                long sum = 0;
                for (ColumnFamilyHandle cf : cfHandles) {
                    long value = get(rocksDB, cf);
                    formatted.add("" + value);
                    sum += value;
                }
                return format("%s (%s)", sum, String.join("/", formatted));
            }

            @Override
            String unitName() {
                return "number";
            }
        }

        private static class Seconds extends Property {

            Seconds(String label, String property, boolean applicablePerCF) {
                super(label, property, applicablePerCF);
            }

            @Override
            String getFormatted(OptimisticTransactionDB rocksDB) throws RocksDBException {
                assert !isApplicablePerColumnFamily();
                return String.format("%s sec", format(get(rocksDB)));
            }

            @Override
            String getFormatted(OptimisticTransactionDB rocksDB, List<ColumnFamilyHandle> cfHandles) throws RocksDBException {
                assert isApplicablePerColumnFamily();
                List<String> formatted = new ArrayList<>();
                long sum = 0;
                for (ColumnFamilyHandle cf : cfHandles) {
                    long value = get(rocksDB, cf);
                    formatted.add(format(value));
                    sum += value;
                }
                return String.format("%s sec (%s)", sum, String.join("/", formatted));
            }

            private String format(long secondsSinceEpoch) {
                double secondsElapsed = 0.0;
                if (secondsSinceEpoch != 0) {
                    secondsElapsed = (System.currentTimeMillis() / 1000.0) - secondsSinceEpoch;
                }
                return String.format("%.2f", secondsElapsed);
            }

            @Override
            String unitName() {
                return "seconds";
            }
        }
    }

    private static String formatBytesToMB(long[] bytes) {
        List<String> formatted = new ArrayList<>();
        long sum = 0;
        for (long b : bytes) {
            formatted.add(toMbString(b));
            sum += b;
        }
        return format("%s mb (%s)", toMbString(sum), String.join("/", formatted));
    }

    private static String toMbString(long bytes) {
        return format("%.1f", ((float) bytes) / MB);
    }

    static class Reader {

        private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Reader.class);

        private final OptimisticTransactionDB rocksDB;
        private final List<ColumnFamilyHandle> cfHandles;

        Reader(OptimisticTransactionDB rocksDB, List<ColumnFamilyHandle> cfHandles) {
            this.rocksDB = rocksDB;
            this.cfHandles = cfHandles;
        }

        long getTotal(Property property) {
            try {
                if (property.isApplicablePerColumnFamily()) {
                    long total = 0;
                    for (ColumnFamilyHandle handle : cfHandles) {
                        total += property.get(rocksDB, handle);
                    }
                    return total;
                } else {
                    return property.get(rocksDB);
                }
            } catch (RocksDBException e) {
                LOG.error(STORAGE_PROPERTY_EXCEPTION.message(), e);
                return -1;
            }
        }

        String getFormatted(Property property) {
            try {
                if (property.isApplicablePerColumnFamily()) {
                    return property.getFormatted(rocksDB, cfHandles);
                } else {
                    return property.getFormatted(rocksDB);
                }
            } catch (RocksDBException e) {
                LOG.error(STORAGE_PROPERTY_EXCEPTION.message(), e);
                return "";
            }
        }
    }

    static class Logger implements Runnable {
        private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Logger.class);

        private final Reader reader;
        private final String database;

        Logger(RocksProperties.Reader reader, String database) {
            this.reader = reader;
            this.database = database;
        }

        @Override
        public void run() {
            logRocksProperties();
            logRocksFilesSummary();
        }

        private void logRocksProperties() {
            StringBuilder builder = new StringBuilder(format(
                    "Database '%s' rocksdb properties from '%d' column families:\n", database, reader.cfHandles.size()));
            for (Property property : RocksProperties.properties.values()) {
                builder.append(format("%-40s %s\n", property.label(), reader.getFormatted(property)));
            }
            LOG.debug(builder.toString());
        }

        private void logRocksFilesSummary() {
            try {
                long[] dataBlocksSize = new long[reader.cfHandles.size()];
                Arrays.fill(dataBlocksSize, 0);
                long[] indexBlocksSize = new long[reader.cfHandles.size()];
                Arrays.fill(indexBlocksSize, 0);
                long[] filterBlocksSize = new long[reader.cfHandles.size()];
                Arrays.fill(filterBlocksSize, 0);
                long[] rawKeysSize = new long[reader.cfHandles.size()];
                Arrays.fill(rawKeysSize, 0);
                long[] rawValuesSize = new long[reader.cfHandles.size()];
                Arrays.fill(rawValuesSize, 0);
                long[] numEntries = new long[reader.cfHandles.size()];
                Arrays.fill(numEntries, 0);
                long[] numDeletions = new long[reader.cfHandles.size()];
                Arrays.fill(numDeletions, 0);
                long[] keysEstimate = new long[reader.cfHandles.size()];
                long sstFiles = 0;
                for (int i = 0; i < reader.cfHandles.size(); i++) {
                    ColumnFamilyHandle cfHandle = reader.cfHandles.get(i);
                    Map<String, TableProperties> sstProperties = reader.rocksDB.getPropertiesOfAllTables(cfHandle);
                    sstFiles += sstProperties.size();
                    for (Map.Entry<String, TableProperties> entry : sstProperties.entrySet()) {
                        TableProperties properties = entry.getValue();
                        dataBlocksSize[i] += properties.getDataSize();
                        indexBlocksSize[i] += properties.getIndexSize();
                        filterBlocksSize[i] += properties.getFilterSize();
                        rawKeysSize[i] += properties.getRawKeySize();
                        rawValuesSize[i] += properties.getRawValueSize();
                        numEntries[i] += properties.getNumEntries();
                        numDeletions[i] += properties.getNumDeletions();
                    }
                    keysEstimate[i] = numEntries[i] - numDeletions[i];
                }
                String formattedSummary = format("%-40s %s \n", "Data blocks size", formatBytesToMB(dataBlocksSize)) +
                        format("%-40s %s \n", "Index blocks size", formatBytesToMB(indexBlocksSize)) +
                        format("%-40s %s \n", "Filter blocks size", formatBytesToMB(filterBlocksSize)) +
                        format("%-40s %s \n", "Raw keys size", formatBytesToMB(rawKeysSize)) +
                        format("%-40s %s \n", "Raw values size", formatBytesToMB(rawValuesSize)) +
                        format("%-40s %d (%s) \n", "Approximate total keys", stream(keysEstimate).sum(),
                                stream(keysEstimate).mapToObj(l -> "" + l).collect(Collectors.joining("/"))) +
                        format("%-40s %s bytes\n", "Bytes per key (raw values/approx keys)",
                                formatSeparated1f(rawKeysSize, (v, i) -> (float) v / (keysEstimate[i]))) +
                        format("%-40s %s bytes\n", "Bytes per value (raw values/approx keys)",
                                formatSeparated1f(rawValuesSize, (v, i) -> (float) v / (keysEstimate[i])));

                LOG.debug("Database '{}' rocksdb summary from '{}' column families and '{}' SST files:\n{}\n",
                        database, reader.cfHandles.size(), sstFiles, formattedSummary);
            } catch (RocksDBException e) {
                LOG.error(STORAGE_PROPERTY_EXCEPTION.message(), e);
            }
        }

        private String formatSeparated1f(long[] values, BiFunction<Long, Integer, Float> preprocessor) {
            List<String> formatted = new ArrayList<>();
            for (int i = 0; i < values.length; i++) {
                formatted.add(format("%.1f", preprocessor.apply(values[i], i)));
            }
            return String.join("/", formatted);
        }
    }
}
