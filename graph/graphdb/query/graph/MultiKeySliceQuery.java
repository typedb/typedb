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

package grakn.core.graph.graphdb.query.graph;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.BackendTransaction;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.keycolumnvalue.KeySliceQuery;
import grakn.core.graph.graphdb.query.BackendQuery;
import grakn.core.graph.graphdb.query.BaseQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MultiKeySliceQuery extends BaseQuery implements BackendQuery<MultiKeySliceQuery> {

    private final List<KeySliceQuery> queries;

    public MultiKeySliceQuery(List<KeySliceQuery> queries) {
        Preconditions.checkArgument(queries != null && !queries.isEmpty());
        this.queries = queries;
    }

    @Override
    public MultiKeySliceQuery updateLimit(int newLimit) {
        MultiKeySliceQuery newQuery = new MultiKeySliceQuery(queries);
        newQuery.setLimit(newLimit);
        return newQuery;
    }

    public List<EntryList> execute(BackendTransaction tx) {
        int total = 0;
        final List<EntryList> result = new ArrayList<>(Math.min(getLimit(), queries.size()));
        for (KeySliceQuery ksq : queries) {
            EntryList next = tx.indexQuery(ksq.updateLimit(getLimit() - total));
            result.add(next);
            total += next.size();
            if (total >= getLimit()) break;
        }
        return result;
    }

    @Override
    public int hashCode() {
        return Objects.hash(queries, getLimit());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other == null) {
            return false;
        } else if (!getClass().isInstance(other)) {
            return false;
        }
        MultiKeySliceQuery oth = (MultiKeySliceQuery) other;
        return getLimit() == oth.getLimit() && queries.equals(oth.queries);
    }

    @Override
    public String toString() {
        return "multiKSQ[" + queries.size() + "]@" + getLimit();
    }

}
