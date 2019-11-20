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

package grakn.core.graph.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.BackendException;
import grakn.core.graph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import grakn.core.graph.diskstorage.keycolumnvalue.keyvalue.KeyValueStore;


public abstract class BaseKeyColumnValueAdapter implements KeyColumnValueStore {

    private final KeyValueStore store;
    private boolean isClosed = false;

    public BaseKeyColumnValueAdapter(KeyValueStore store) {
        Preconditions.checkNotNull(store);
        this.store = store;
    }

    @Override
    public String getName() {
        return store.getName();
    }

    @Override
    public void close() throws BackendException {
        store.close();
        isClosed=true;
    }

    public boolean isClosed() {
        return isClosed;
    }


}
