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

package grakn.core.graph.diskstorage.util;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyIterator;
import grakn.core.graph.diskstorage.keycolumnvalue.KeySliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.StoreTransaction;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;

/**
 * This class is used by {@code MetricInstrumentedStore} to measure wall clock
 * time, method invocation counts, and exceptions thrown by the methods on
 * {@link RecordIterator} instances returned from
 * {@link MetricInstrumentedStore#getSlice(KeySliceQuery, StoreTransaction)}.
 */
public class MetricInstrumentedIterator implements KeyIterator {

    private final KeyIterator iterator;
    private final String p;

    private static final String M_HAS_NEXT = "hasNext";
    private static final String M_NEXT = "next";
    static final String M_CLOSE = "close";

    /**
     * If the iterator argument is non-null, then return a new
     * {@code MetricInstrumentedIterator} wrapping it. Metrics for method calls
     * on the wrapped instance will be prefixed with the string {@code prefix}
     * which must be non-null. If the iterator argument is null, then return
     * null.
     *
     * @param keyIterator the iterator to wrap with Metrics measurements
     * @param prefix      the Metrics name prefix string
     * @return a wrapper around {@code keyIterator} or null if
     * {@code keyIterator} is null
     */
    public static MetricInstrumentedIterator of(KeyIterator keyIterator, String... prefix) {
        if (keyIterator == null) {
            return null;
        }

        Preconditions.checkNotNull(prefix);
        return new MetricInstrumentedIterator(keyIterator, StringUtils.join(prefix, "."));
    }

    private MetricInstrumentedIterator(KeyIterator i, String p) {
        this.iterator = i;
        this.p = p;
    }

    @Override
    public boolean hasNext() {
        return MetricInstrumentedStore.runWithMetrics(p, M_HAS_NEXT,
                (UncheckedCallable<Boolean>) iterator::hasNext);
    }

    @Override
    public StaticBuffer next() {
        return MetricInstrumentedStore.runWithMetrics(p, M_NEXT,
                (UncheckedCallable<StaticBuffer>) iterator::next);
    }

    @Override
    public void close() throws IOException {
        MetricInstrumentedStore.runWithMetrics(p, MetricInstrumentedIterator.M_CLOSE, (IOCallable<Void>) () -> {
            iterator.close();
            return null;
        });
    }

    @Override
    public RecordIterator<Entry> getEntries() {
        // TODO: add metrics to entries if ever needed
        return iterator.getEntries();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
