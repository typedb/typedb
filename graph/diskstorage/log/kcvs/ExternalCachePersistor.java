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

package grakn.core.graph.diskstorage.log.kcvs;

import com.google.common.collect.Lists;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.KCVSCache;


public class ExternalCachePersistor implements ExternalPersistor {

    private final KCVSCache kcvs;
    private final CacheTransaction tx;

    public ExternalCachePersistor(KCVSCache kcvs, CacheTransaction tx) {
        this.kcvs = kcvs;
        this.tx = tx;
    }

    @Override
    public void add(StaticBuffer key, Entry cell) {
        try {
            kcvs.mutateEntries(key, Lists.newArrayList(cell), KCVSCache.NO_DELETIONS, tx);
        } catch (BackendException e) {
            throw new JanusGraphException("Unexpected storage exception in LOG persistence against cache", e);
        }
    }
}
