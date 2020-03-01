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

package grakn.core.graph.graphdb.transaction.addedrelations;

import grakn.core.graph.graphdb.internal.InternalRelation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;


public class SimpleBufferAddedRelations implements AddedRelationsContainer {

    private static final int INITIAL_ADDED_SIZE = 10;
    private static final int INITIAL_DELETED_SIZE = 10;
    private static final int MAX_DELETED_SIZE = 500;

    private List<InternalRelation> added;
    private List<InternalRelation> deleted;

    public SimpleBufferAddedRelations() {
        added = new ArrayList<>(INITIAL_ADDED_SIZE);
        deleted = null;
    }

    @Override
    public boolean add(InternalRelation relation) {
        return added.add(relation);
    }

    @Override
    public boolean remove(InternalRelation relation) {
        if (added.isEmpty()) return false;
        if (deleted == null) deleted = new ArrayList<>(INITIAL_DELETED_SIZE);
        boolean del = deleted.add(relation);
        if (deleted.size() > MAX_DELETED_SIZE) cleanup();
        return del;
    }

    @Override
    public boolean isEmpty() {
        cleanup();
        return added.isEmpty();
    }

    private void cleanup() {
        if (deleted == null || deleted.isEmpty()) return;
        final Set<InternalRelation> deletedSet = new HashSet<>(deleted);
        deleted = null;
        final List<InternalRelation> newlyAdded = new ArrayList<>(added.size() - deletedSet.size() / 2);
        for (InternalRelation r : added) {
            if (!deletedSet.contains(r)) newlyAdded.add(r);
        }
        added = newlyAdded;
    }

    @Override
    public List<InternalRelation> getView(Predicate<InternalRelation> filter) {
        cleanup();
        final List<InternalRelation> result = new ArrayList<>();
        for (InternalRelation r : added) {
            if (filter.test(r)) result.add(r);
        }
        return result;
    }

    @Override
    public Collection<InternalRelation> getAll() {
        cleanup();
        return added;
    }
}
