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

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.AbstractIterator;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyIterator;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.diskstorage.util.RecordIterator;
import grakn.core.graph.diskstorage.util.StaticArrayBuffer;
import grakn.core.graph.diskstorage.util.StaticArrayEntry;
import io.vavr.Tuple;
import io.vavr.Tuple3;
import io.vavr.collection.Iterator;

import java.io.IOException;

/**
 * SliceQuery iterator that handles CQL result sets that may have more
 * data returned in each column than the SliceQuery has configured as
 * it's limit. I.e. the iterator only returns the number of entries for each Key
 * to the number of Columns specified in the SliceQuerys limit.
 */
class CQLResultSetKeyIterator extends AbstractIterator<StaticBuffer> implements KeyIterator {

    private final SliceQuery sliceQuery;
    private final CQLColValGetter getter;
    private final Iterator<Row> iterator;

    private Row currentRow = null;
    private StaticBuffer currentKey = null;
    private StaticBuffer lastKey = null;

    CQLResultSetKeyIterator(SliceQuery sliceQuery, CQLColValGetter getter, ResultSet resultSet) {
        this.sliceQuery = sliceQuery;
        this.getter = getter;
        this.iterator = Iterator.ofAll(resultSet.iterator())
                .peek(row -> {
                    this.currentRow = row;
                    this.currentKey = StaticArrayBuffer.of(row.getByteBuffer(CQLKeyColumnValueStore.KEY_COLUMN_NAME));
                });
    }

    @Override
    protected StaticBuffer computeNext() {
        if (this.currentKey != null && !this.currentKey.equals(this.lastKey)) {
            this.lastKey = this.currentKey;
            return this.lastKey;
        }

        while (this.iterator.hasNext()) {
            this.iterator.next();
            if (this.currentKey != null && !this.currentKey.equals(this.lastKey)) {
                this.lastKey = this.currentKey;
                return this.lastKey;
            }
        }
        return endOfData();
    }

    @Override
    public RecordIterator<Entry> getEntries() {
        return new EntryRecordIterator(this.sliceQuery, this.getter, Iterator.of(this.currentRow).concat(this.iterator), this.currentKey);
    }

    @Override
    public void close() throws IOException {
        // NOP
    }

    static class EntryRecordIterator extends AbstractIterator<Entry> implements RecordIterator<Entry> {

        private final CQLColValGetter getter;
        private final Iterator<Tuple3<StaticBuffer, StaticBuffer, Row>> iterator;

        EntryRecordIterator(SliceQuery sliceQuery, CQLColValGetter getter, Iterator<Row> iterator, StaticBuffer key) {
            this.getter = getter;
            StaticBuffer sliceEnd = sliceQuery.getSliceEnd();
            this.iterator = iterator
                    .<Tuple3<StaticBuffer, StaticBuffer, Row>> map(row -> Tuple.of(
                            StaticArrayBuffer.of(row.getByteBuffer(CQLKeyColumnValueStore.COLUMN_COLUMN_NAME)),
                            StaticArrayBuffer.of(row.getByteBuffer(CQLKeyColumnValueStore.VALUE_COLUMN_NAME)),
                            row))
                    .takeWhile(tuple -> key.equals(StaticArrayBuffer.of(tuple._3.getByteBuffer(CQLKeyColumnValueStore.KEY_COLUMN_NAME))) && !sliceEnd.equals(tuple._1))
                    .take(sliceQuery.getLimit());
        }

        @Override
        protected Entry computeNext() {
            if (this.iterator.hasNext()) {
                return StaticArrayEntry.ofStaticBuffer(this.iterator.next(), this.getter);
            }
            return endOfData();
        }

        @Override
        public void close() throws IOException {
            // NOP
        }
    }
}
