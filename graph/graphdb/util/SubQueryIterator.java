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

package grakn.core.graph.graphdb.util;

import com.google.common.cache.Cache;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.diskstorage.BackendTransaction;
import grakn.core.graph.graphdb.database.IndexSerializer;
import grakn.core.graph.graphdb.query.graph.JointIndexQuery;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class SubQueryIterator implements Iterator<JanusGraphElement>, AutoCloseable {

    private final JointIndexQuery.Subquery subQuery;

    private final Cache<JointIndexQuery.Subquery, List<Object>> indexCache;

    private Iterator<? extends JanusGraphElement> elementIterator;

    private List<Object> currentIds;

    private QueryProfiler profiler;

    private boolean isTimerRunning;

    public SubQueryIterator(JointIndexQuery.Subquery subQuery, IndexSerializer indexSerializer, BackendTransaction tx,
                            Cache<JointIndexQuery.Subquery, List<Object>> indexCache, int limit,
                            Function<Object, ? extends JanusGraphElement> function, List<Object> otherResults) {
        this.subQuery = subQuery;
        this.indexCache = indexCache;
        List<Object> cacheResponse = indexCache.getIfPresent(subQuery);
        Stream<?> stream;
        if (cacheResponse != null) {
            stream = cacheResponse.stream();
        } else {
            try {
                currentIds = new ArrayList<>();
                profiler = QueryProfiler.startProfile(subQuery.getProfiler(), subQuery);
                isTimerRunning = true;
                stream = indexSerializer.query(subQuery, tx).peek(r -> currentIds.add(r));
            } catch (Exception e) {
                throw new JanusGraphException("Could not call index", e.getCause());
            }
        }
        elementIterator = stream.filter(e -> otherResults == null || otherResults.contains(e)).limit(limit).map(function).map(r -> (JanusGraphElement) r).iterator();
    }

    @Override
    public boolean hasNext() {
        if (!elementIterator.hasNext() && currentIds != null) {
            indexCache.put(subQuery, currentIds);
            profiler.stopTimer();
            isTimerRunning = false;
            profiler.setResultSize(currentIds.size());
        }
        return elementIterator.hasNext();
    }

    @Override
    public JanusGraphElement next() {
        return this.elementIterator.next();
    }

    @Override
    public void close() throws Exception {
        if (isTimerRunning) {
            profiler.stopTimer();
        }
    }

}
