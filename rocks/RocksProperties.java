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

    private static final List<Property> properties = list(
            new Property.Byte("Active memtable size", "rocksdb.cur-size-active-mem-table", true),
            new Property.Byte("All memtables size", "rocksdb.size-all-mem-tables", true),
            new Property.Byte("Block cache capacity", "rocksdb.block-cache-capacity", false),
            new Property.Byte("Block cache usage", "rocksdb.block-cache-usage", false),
            new Property.Byte("Block cache pinned usage", "rocksdb.block-cache-pinned-usage", false),
            new Property.Byte("Est. SST readers pinned memory", "rocksdb.estimate-table-readers-mem", true),
            new Property.Number("Active snapshots", "rocksdb.num-snapshots", false),
            new Property.Seconds("Oldest active snapshot", "rocksdb.oldest-snapshot-time", false),
            new Property.Number("Live LSM tree versions", "rocksdb.num-live-versions", true),
            new Property.Byte("Live SST files size", "rocksdb.live-sst-files-size", true),
            new Property.Byte("SST files held size", "rocksdb.total-sst-files-size", true),
            new Property.Byte("Pending memtable flush size", "rocksdb.mem-table-flush-pending", true),
            new Property.Byte("Estimated pending compaction size", "rocksdb.estimate-pending-compaction-bytes", true),
            new Property.Number("Running compactions", "rocksdb.num-running-compactions", true),
            new Property.Number("Running flushes", "rocksdb.num-running-flushes", true)
    );

    private static abstract class Property {

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

    static class Logger implements Runnable {

        private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RocksProperties.class);

        private final OptimisticTransactionDB rocksDB;
        private final List<ColumnFamilyHandle> cfHandles;
        private final String database;

        Logger(OptimisticTransactionDB rocksDB, List<ColumnFamilyHandle> cfHandles, String database) {
            this.rocksDB = rocksDB;
            this.cfHandles = cfHandles;
            this.database = database;
        }

        @Override
        public void run() {
            logRocksProperties();
            logRocksFilesSummary();
        }

        private void logRocksProperties() {
            try {
                StringBuilder builder = new StringBuilder(format(
                        "Database '%s' rocksdb properties from '%d' column families:\n", database, cfHandles.size()));
                for (Property property : RocksProperties.properties) {
                    if (property.isApplicablePerColumnFamily()) {
                        builder.append(format("%-40s %s\n", property.label(), property.getFormatted(rocksDB, cfHandles)));
                    } else {
                        builder.append(format("%-40s %s\n", property.label(), property.getFormatted(rocksDB)));
                    }
                }
                LOG.debug(builder.toString());
            } catch (RocksDBException e) {
                LOG.error(STORAGE_PROPERTY_EXCEPTION.message(), e);
            }
        }

        private void logRocksFilesSummary() {
            try {
                long[] dataBlocksSize = new long[cfHandles.size()];
                Arrays.fill(dataBlocksSize, 0);
                long[] indexBlocksSize = new long[cfHandles.size()];
                Arrays.fill(indexBlocksSize, 0);
                long[] filterBlocksSize = new long[cfHandles.size()];
                Arrays.fill(filterBlocksSize, 0);
                long[] rawKeysSize = new long[cfHandles.size()];
                Arrays.fill(rawKeysSize, 0);
                long[] rawValuesSize = new long[cfHandles.size()];
                Arrays.fill(rawValuesSize, 0);
                long[] numEntries = new long[cfHandles.size()];
                Arrays.fill(numEntries, 0);
                long[] numDeletions = new long[cfHandles.size()];
                Arrays.fill(numDeletions, 0);
                long[] keysEstimate = new long[cfHandles.size()];
                long sstFiles = 0;
                for (int i = 0; i < cfHandles.size(); i++) {
                    ColumnFamilyHandle cfHandle = cfHandles.get(i);
                    Map<String, TableProperties> sstProperties = rocksDB.getPropertiesOfAllTables(cfHandle);
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
                        database, cfHandles.size(), sstFiles, formattedSummary);
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
