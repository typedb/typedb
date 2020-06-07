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

import com.google.common.base.Preconditions;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.graphdb.internal.ElementLifeCycle;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.transaction.addedrelations.AddedRelationsContainer;
import grakn.core.graph.graphdb.transaction.addedrelations.ConcurrentAddedRelations;
import grakn.core.graph.graphdb.transaction.addedrelations.SimpleAddedRelations;
import grakn.core.graph.util.datastructures.Retriever;

import java.util.List;
import java.util.function.Predicate;


public class StandardVertex extends AbstractVertex {

    private final Object lifecycleMutex = new Object();
    private volatile byte lifecycle;
    private volatile AddedRelationsContainer addedRelations = AddedRelationsContainer.EMPTY;

    public StandardVertex(StandardJanusGraphTx tx, long id, byte lifecycle) {
        super(tx, id);
        this.lifecycle = lifecycle;
    }

    public final void updateLifeCycle(ElementLifeCycle.Event event) {
        synchronized (lifecycleMutex) {
            this.lifecycle = ElementLifeCycle.update(lifecycle, event);
        }
    }

    @Override
    public void removeRelation(InternalRelation r) {
        if (r.isNew()) {
            addedRelations.remove(r);
        } else if (r.isLoaded()) {
            updateLifeCycle(ElementLifeCycle.Event.REMOVED_RELATION);
        } else {
            throw new IllegalArgumentException("Unexpected relation status: " + r.isRemoved());
        }
    }

    @Override
    public boolean addRelation(InternalRelation r) {
        Preconditions.checkArgument(r.isNew());
        if (addedRelations == AddedRelationsContainer.EMPTY) {
            if (tx().getConfiguration().isSingleThreaded()) {
                addedRelations = new SimpleAddedRelations();
            } else {
                synchronized (this) {
                    if (addedRelations == AddedRelationsContainer.EMPTY) {
                        addedRelations = new ConcurrentAddedRelations();
                    }
                }
            }
        }
        if (addedRelations.add(r)) {
            updateLifeCycle(ElementLifeCycle.Event.ADDED_RELATION);
            return true;
        } else return false;
    }

    @Override
    public List<InternalRelation> getAddedRelations(Predicate<InternalRelation> query) {
        return addedRelations.getView(query);
    }

    @Override
    public EntryList loadRelations(SliceQuery query, Retriever<SliceQuery, EntryList> lookup) {
        return (isNew()) ? EntryList.EMPTY_LIST : lookup.get(query);
    }

    @Override
    public boolean hasLoadedRelations(SliceQuery query) {
        return false;
    }

    @Override
    public boolean hasRemovedRelations() {
        return ElementLifeCycle.hasRemovedRelations(lifecycle);
    }

    @Override
    public boolean hasAddedRelations() {
        return ElementLifeCycle.hasAddedRelations(lifecycle);
    }

    @Override
    public synchronized void remove() {
        super.remove();
        ((StandardVertex) it()).updateLifeCycle(ElementLifeCycle.Event.REMOVED);
    }

    @Override
    public byte getLifeCycle() {
        return lifecycle;
    }
}
