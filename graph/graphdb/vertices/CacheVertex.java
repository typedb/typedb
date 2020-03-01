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

package grakn.core.graph.graphdb.vertices;

import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.util.datastructures.Retriever;

import java.util.HashMap;
import java.util.Map;


public class CacheVertex extends StandardVertex {
    // We don't try to be smart and match with previous queries
    // because that would waste more cycles on lookup than save actual memory
    // We use a normal map with synchronization since the likelihood of contention
    // is super low in a single transaction
    private final Map<SliceQuery, EntryList> queryCache;

    public CacheVertex(StandardJanusGraphTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
        queryCache = new HashMap<>(4);
    }

    protected void addToQueryCache(SliceQuery query, EntryList entries) {
        synchronized (queryCache) {
            //TODO: become smarter about what to cache and when (e.g. memory pressure)
            queryCache.put(query, entries);
        }
    }

    protected int getQueryCacheSize() {
        synchronized (queryCache) {
            return queryCache.size();
        }
    }

    @Override
    public EntryList loadRelations(SliceQuery query, Retriever<SliceQuery, EntryList> lookup) {
        if (isNew()) {
            return EntryList.EMPTY_LIST;
        }
        EntryList result;
        synchronized (queryCache) {
            result = queryCache.get(query);
        }
        if (result == null) {
            //First check for super
            Map.Entry<SliceQuery, EntryList> superset = getSuperResultSet(query);
            if (superset == null || superset.getValue() == null) {
                result = lookup.get(query);
            } else {
                result = query.getSubset(superset.getKey(), superset.getValue());
            }
            addToQueryCache(query, result);

        }
        return result;
    }

    @Override
    public boolean hasLoadedRelations(SliceQuery query) {
        synchronized (queryCache) {
            return queryCache.get(query) != null || getSuperResultSet(query) != null;
        }
    }

    private Map.Entry<SliceQuery, EntryList> getSuperResultSet(SliceQuery query) {

        synchronized (queryCache) {
            if (queryCache.size() > 0) {
                for (Map.Entry<SliceQuery, EntryList> entry : queryCache.entrySet()) {
                    if (entry.getKey().subsumes(query)) return entry;
                }
            }
        }
        return null;
    }

}
